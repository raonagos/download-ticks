//! Decoding of the archived CSV objects into canonical tape events.
//!
//! Each object is decoded on its own thread and streamed through a bounded
//! channel, so a day of `bookTicker` (20 million rows) never sits in memory.
//! Every decoder validates the row before emitting: a crossed book, a
//! non-positive size or a non-finite number is rejected and counted, never
//! silently repaired.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::Instant;

use chrono::NaiveDateTime;

use crate::archive::Product;
use crate::errors::{Error, Result};

/// Event kinds, ranked in the order the tape sorts them within one timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    BookDepth = 0,
    BookTicker = 1,
    Funding = 2,
    Kline = 3,
    Trade = 4,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::BookDepth => "book_depth",
            Kind::BookTicker => "book_ticker",
            Kind::Funding => "funding",
            Kind::Kline => "kline",
            Kind::Trade => "trade",
        }
    }
}

/// One depth band, as archived: distance from mid in percent.
#[derive(Debug, Clone, Copy)]
pub struct Band {
    pub percentage: i32,
    pub depth: f64,
    pub notional: f64,
}

/// The payload of an event.
#[derive(Debug, Clone)]
pub enum Payload {
    Trade {
        price: f64,
        size: f64,
        side: &'static str,
    },
    BookTicker {
        bid: f64,
        bid_size: f64,
        ask: f64,
        ask_size: f64,
        update_id: u64,
        transaction_ts: i64,
    },
    BookDepth {
        bands: Vec<Band>,
    },
    Kline {
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        taker_buy: f64,
        trades: u64,
    },
    Funding {
        rate: f64,
        stamp: i64,
    },
}

/// One canonical tape event.
#[derive(Debug, Clone)]
pub struct Event {
    pub ts: i64,
    pub kind: Kind,
    pub id: u64,
    pub payload: Payload,
}

impl Event {
    /// Appends the JSON line, without the trailing newline.
    ///
    /// Written by hand because the tape is the hot path: twenty million lines a
    /// day, and `serde_json` per line doubles the cost for no gain.
    pub fn write_json(&self, symbol: &str, out: &mut String) {
        use std::fmt::Write as _;
        out.push_str("{\"ts\":");
        let _ = write!(out, "{}", self.ts);
        out.push_str(",\"kind\":\"");
        out.push_str(self.kind.as_str());
        out.push_str("\",\"id\":");
        let _ = write!(out, "{}", self.id);
        out.push_str(",\"venue\":\"binance\",\"market\":\"um\",\"symbol\":\"");
        out.push_str(symbol);
        out.push('"');

        match &self.payload {
            Payload::Trade { price, size, side } => {
                out.push_str(",\"price\":");
                write_f64(out, *price);
                out.push_str(",\"size\":");
                write_f64(out, *size);
                out.push_str(",\"side\":\"");
                out.push_str(side);
                out.push('"');
            }
            Payload::BookTicker {
                bid,
                bid_size,
                ask,
                ask_size,
                update_id,
                transaction_ts,
            } => {
                out.push_str(",\"bid\":");
                write_f64(out, *bid);
                out.push_str(",\"bid_size\":");
                write_f64(out, *bid_size);
                out.push_str(",\"ask\":");
                write_f64(out, *ask);
                out.push_str(",\"ask_size\":");
                write_f64(out, *ask_size);
                out.push_str(",\"update_id\":");
                let _ = write!(out, "{update_id}");
                out.push_str(",\"transaction_ts\":");
                let _ = write!(out, "{transaction_ts}");
            }
            Payload::BookDepth { bands } => {
                out.push_str(",\"bands\":[");
                for (index, band) in bands.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"percentage\":");
                    let _ = write!(out, "{}", band.percentage);
                    out.push_str(",\"depth\":");
                    write_f64(out, band.depth);
                    out.push_str(",\"notional\":");
                    write_f64(out, band.notional);
                    out.push('}');
                }
                out.push(']');
            }
            Payload::Kline {
                open,
                high,
                low,
                close,
                volume,
                taker_buy,
                trades,
            } => {
                out.push_str(",\"interval\":\"1m\",\"open\":");
                write_f64(out, *open);
                out.push_str(",\"high\":");
                write_f64(out, *high);
                out.push_str(",\"low\":");
                write_f64(out, *low);
                out.push_str(",\"close\":");
                write_f64(out, *close);
                out.push_str(",\"volume\":");
                write_f64(out, *volume);
                out.push_str(",\"taker_buy\":");
                write_f64(out, *taker_buy);
                out.push_str(",\"trades\":");
                let _ = write!(out, "{trades}");
            }
            Payload::Funding { rate, stamp } => {
                out.push_str(",\"rate\":");
                write_f64(out, *rate);
                out.push_str(",\"stamp\":");
                let _ = write!(out, "{stamp}");
            }
        }
        out.push('}');
    }

    /// The merge key: `(ts, kind, id)`, per data-contract §4.
    pub fn key(&self) -> (i64, u8, u64) {
        (self.ts, self.kind as u8, self.id)
    }
}

fn write_f64(out: &mut String, value: f64) {
    use std::fmt::Write as _;
    // Rust's shortest round-trip formatting is valid JSON for finite values,
    // and the decoders reject non-finite ones before an event is built.
    let _ = write!(out, "{value}");
}

/// Counters for one decoded object.
#[derive(Debug, Default, Clone, Copy)]
pub struct DecodeStats {
    pub rows: u64,
    pub rejected: u64,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub elapsed_ms: u128,
}

impl DecodeStats {
    fn observe(&mut self, ts: i64) {
        self.rows += 1;
        self.min_ts = Some(self.min_ts.map_or(ts, |min| min.min(ts)));
        self.max_ts = Some(self.max_ts.map_or(ts, |max| max.max(ts)));
    }
}

/// A running decoder: a bounded channel and the thread feeding it.
pub struct Decoder {
    pub rx: Receiver<Result<Event>>,
    pub handle: JoinHandle<Result<DecodeStats>>,
}

impl Decoder {
    /// Closes the channel and joins the thread, returning its statistics.
    pub fn finish(self) -> Result<DecodeStats> {
        drop(self.rx);
        self.handle.join().map_err(|_| Error::DecoderPanicked)?
    }
}

/// Starts decoding one archived object on its own thread.
///
/// At most `capacity` events are buffered, so the producer blocks on the
/// consumer instead of filling memory.
pub fn spawn(product: Product, path: &Path, capacity: usize) -> Result<Decoder> {
    let (tx, rx) = sync_channel(capacity);
    let path = path.to_path_buf();
    let handle = std::thread::Builder::new()
        .name(format!("decode-{}", product.as_str()))
        .spawn(move || decode_object(product, &path, &tx))?;
    Ok(Decoder { rx, handle })
}

fn decode_object(product: Product, path: &Path, tx: &SyncSender<Result<Event>>) -> Result<DecodeStats> {
    decode_with(product, path, |event| tx.send(Ok(event)).is_ok())
}

/// Decodes one object, handing every event to `emit`; stops when it returns false.
pub fn decode_with(product: Product, path: &Path, mut emit: impl FnMut(Event) -> bool) -> Result<DecodeStats> {
    let started = Instant::now();
    let file = File::open(path)?;
    let mut archive = zip::ZipArchive::new(BufReader::with_capacity(1 << 20, file)).map_err(Error::Zip)?;
    if archive.len() != 1 {
        return Err(Error::ArchiveShape {
            path: path.to_path_buf(),
            members: archive.len(),
        });
    }
    let member = archive.by_index(0).map_err(Error::Zip)?;
    let reader: Box<dyn Read> = Box::new(member);
    let mut lines = BufReader::with_capacity(1 << 20, reader).lines();

    // The header is present on recent files and absent on old ones; detect it.
    let first = lines.next().transpose()?.unwrap_or_default();
    let mut stats = DecodeStats::default();
    let mut rejected = 0u64;
    if !looks_like_header(&first) {
        match parse_line(product, &first) {
            Ok(Some(event)) => {
                stats.observe(event.ts);
                if !emit(event) {
                    return Err(Error::ChannelClosed);
                }
            }
            Ok(None) => rejected += 1,
            Err(_) => rejected += 1,
        }
    }

    match product {
        Product::BookDepth => {
            let mut group = DepthGroup::default();
            for line in lines {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                match group.push(&line, &mut stats) {
                    Ok(Some(events)) => {
                        for event in events {
                            if !emit(event) {
                                return Err(Error::ChannelClosed);
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(_) => rejected += 1,
                }
            }
            if let Some(event) = group.flush(&mut stats)? {
                let _ = emit(event);
            }
        }
        _ => {
            for line in lines {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                match parse_line(product, &line) {
                    Ok(Some(event)) => {
                        stats.observe(event.ts);
                        if !emit(event) {
                            return Err(Error::ChannelClosed);
                        }
                    }
                    Ok(None) | Err(_) => rejected += 1,
                }
            }
        }
    }

    stats.rejected = rejected;
    stats.elapsed_ms = started.elapsed().as_millis();
    Ok(stats)
}

fn looks_like_header(line: &str) -> bool {
    !line.starts_with(|c: char| c.is_ascii_digit() || c == '-')
}

/// Parses one CSV row into an event. `Ok(None)` means a valid row we drop,
/// `Err` a malformed row.
fn parse_line(product: Product, line: &str) -> Result<Option<Event>> {
    let mut fields = line.split(',');
    match product {
        Product::AggTrades => {
            let id = parse_u64(fields.next())?;
            let price = parse_f64(fields.next())?;
            let size = parse_f64(fields.next())?;
            let _first = fields.next();
            let _last = fields.next();
            let ts = parse_i64(fields.next())?;
            let is_buyer_maker = fields.next().unwrap_or("false");
            if !(price > 0.0 && size > 0.0) {
                return Ok(None);
            }
            Ok(Some(Event {
                ts,
                kind: Kind::Trade,
                id,
                payload: Payload::Trade {
                    price,
                    size,
                    side: if is_buyer_maker == "true" { "sell" } else { "buy" },
                },
            }))
        }
        Product::BookTicker => {
            let update_id = parse_u64(fields.next())?;
            let bid = parse_f64(fields.next())?;
            let bid_size = parse_f64(fields.next())?;
            let ask = parse_f64(fields.next())?;
            let ask_size = parse_f64(fields.next())?;
            let transaction_ts = parse_i64(fields.next())?;
            let ts = parse_i64(fields.next())?;
            if !(bid > 0.0 && ask > 0.0 && bid_size > 0.0 && ask_size > 0.0) || bid >= ask {
                return Ok(None);
            }
            Ok(Some(Event {
                ts,
                kind: Kind::BookTicker,
                id: update_id,
                payload: Payload::BookTicker {
                    bid,
                    bid_size,
                    ask,
                    ask_size,
                    update_id,
                    transaction_ts,
                },
            }))
        }
        Product::Klines => {
            let open_time = parse_i64(fields.next())?;
            let open = parse_f64(fields.next())?;
            let high = parse_f64(fields.next())?;
            let low = parse_f64(fields.next())?;
            let close = parse_f64(fields.next())?;
            let volume = parse_f64(fields.next())?;
            let close_time = parse_i64(fields.next())?;
            let _quote = fields.next();
            let trades = parse_u64(fields.next()).unwrap_or(0);
            let taker_buy = parse_f64(fields.next()).unwrap_or(0.0);
            if !(open > 0.0 && high > 0.0 && low > 0.0 && close > 0.0) {
                return Ok(None);
            }
            Ok(Some(Event {
                ts: close_time,
                kind: Kind::Kline,
                id: open_time as u64,
                payload: Payload::Kline {
                    open,
                    high,
                    low,
                    close,
                    volume,
                    taker_buy,
                    trades,
                },
            }))
        }
        Product::BookDepth => unreachable!("bookDepth is grouped, not parsed line by line"),
    }
}

/// Groups ten `bookDepth` rows into one snapshot event.
#[derive(Default)]
struct DepthGroup {
    stamp: Option<String>,
    bands: Vec<Band>,
}

impl DepthGroup {
    fn push(&mut self, line: &str, stats: &mut DecodeStats) -> Result<Option<Vec<Event>>> {
        let mut fields = line.split(',');
        let stamp = fields.next().ok_or(Error::MalformedRow)?.to_owned();
        let percentage = fields
            .next()
            .ok_or(Error::MalformedRow)?
            .parse::<i32>()
            .map_err(|_| Error::MalformedRow)?;
        let depth = parse_f64(fields.next())?;
        let notional = parse_f64(fields.next())?;
        if !(depth > 0.0 && notional > 0.0) {
            return Ok(Some(Vec::new()));
        }

        match &self.stamp {
            Some(current) if *current == stamp => {
                self.bands.push(Band { percentage, depth, notional });
                Ok(None)
            }
            _ => {
                let previous = self.flush(stats)?;
                self.stamp = Some(stamp);
                self.bands.push(Band { percentage, depth, notional });
                Ok(previous.map(|event| vec![event]))
            }
        }
    }

    fn flush(&mut self, stats: &mut DecodeStats) -> Result<Option<Event>> {
        let Some(stamp) = self.stamp.take() else {
            return Ok(None);
        };
        let bands = std::mem::take(&mut self.bands);
        if bands.len() != 10 {
            return Ok(None);
        }
        let ts = parse_stamp(&stamp)?;
        stats.observe(ts);
        Ok(Some(Event {
            ts,
            kind: Kind::BookDepth,
            id: ts as u64,
            payload: Payload::BookDepth { bands },
        }))
    }
}

fn parse_stamp(value: &str) -> Result<i64> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .map(|moment| moment.and_utc().timestamp_millis())
        .map_err(|_| Error::MalformedRow)
}

fn parse_f64(value: Option<&str>) -> Result<f64> {
    let value = value.ok_or(Error::MalformedRow)?.trim();
    let parsed = value.parse::<f64>().map_err(|_| Error::MalformedRow)?;
    if !parsed.is_finite() {
        return Err(Error::MalformedRow);
    }
    Ok(parsed)
}

fn parse_i64(value: Option<&str>) -> Result<i64> {
    value.ok_or(Error::MalformedRow)?.trim().parse::<i64>().map_err(|_| Error::MalformedRow)
}

fn parse_u64(value: Option<&str>) -> Result<u64> {
    value.ok_or(Error::MalformedRow)?.trim().parse::<u64>().map_err(|_| Error::MalformedRow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_side_comes_from_the_maker_flag() {
        let event = parse_line(Product::AggTrades, "1,41735.0,0.038,2,2,1705276800081,false")
            .unwrap()
            .unwrap();
        assert!(matches!(event.payload, Payload::Trade { side: "buy", .. }));

        let event = parse_line(Product::AggTrades, "2,41734.9,0.036,3,5,1705276800090,true").unwrap().unwrap();
        assert!(matches!(event.payload, Payload::Trade { side: "sell", .. }));
    }

    #[test]
    fn a_crossed_book_is_rejected() {
        // bid == ask
        assert!(
            parse_line(Product::BookTicker, "1,100.0,1.0,100.0,1.0,1705276800014,1705276800018")
                .unwrap()
                .is_none()
        );
        // bid > ask
        assert!(
            parse_line(Product::BookTicker, "1,101.0,1.0,100.0,1.0,1705276800014,1705276800018")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_valid_book_keeps_both_timestamps() {
        let event = parse_line(
            Product::BookTicker,
            "3831603310899,41734.9,0.702,41735.0,2.033,1705276800014,1705276800018",
        )
        .unwrap()
        .unwrap();
        assert_eq!(event.ts, 1_705_276_800_018);
        assert!(matches!(
            event.payload,
            Payload::BookTicker {
                transaction_ts: 1_705_276_800_014,
                ..
            }
        ));
    }

    #[test]
    fn kline_ts_is_the_close_time() {
        let event = parse_line(
            Product::Klines,
            "1705276800000,41734.90,41797.50,41720.00,41763.10,568.072,1705276859999,23720103.72170,6604,283.435,11835014.36850,0",
        )
        .unwrap()
        .unwrap();
        assert_eq!(event.ts, 1_705_276_859_999);
        assert_eq!(event.id, 1_705_276_800_000);
    }

    #[test]
    fn depth_groups_ten_rows_into_one_snapshot() {
        let mut group = DepthGroup::default();
        let mut stats = DecodeStats::default();
        for percentage in [-5, -4, -3, -2, -1, 1, 2, 3, 4] {
            group.push(&format!("2024-01-15 00:00:12,{percentage},10.0,100.0"), &mut stats).unwrap();
        }
        let event = group.push("2024-01-15 00:00:12,5,10.0,100.0", &mut stats).unwrap();
        assert!(event.is_none());
        let event = group.flush(&mut stats).unwrap().unwrap();
        match event.payload {
            Payload::BookDepth { bands } => assert_eq!(bands.len(), 10),
            _ => panic!("expected a depth snapshot"),
        }
        assert_eq!(event.ts, 1_705_276_812_000);
    }

    #[test]
    fn json_line_matches_the_contract_envelope() {
        let mut out = String::new();
        Event {
            ts: 1,
            kind: Kind::Trade,
            id: 7,
            payload: Payload::Trade {
                price: 1.5,
                size: 2.0,
                side: "buy",
            },
        }
        .write_json("BTCUSDT", &mut out);
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["kind"], "trade");
        assert_eq!(value["venue"], "binance");
        assert_eq!(value["market"], "um");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["price"], 1.5);
        assert_eq!(value["side"], "buy");
    }
}
