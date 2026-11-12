//! Building the canonical tape: a k-way merge of one day's decoders.
//!
//! The tape is sorted by `(ts, kind, id)` and compressed with zstd. The merge is
//! streaming: each decoder runs on its own thread behind a bounded channel, and
//! only one event per source is held at a time.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::NaiveDate;

use crate::archive::Product;
use crate::decode::{DecodeStats, Decoder, Event, Kind, Payload, spawn};
use crate::errors::{Error, Result};

/// Where the tape for one day lives.
pub fn tape_path(root: &Path, symbol: &str, date: NaiveDate) -> PathBuf {
    root.join("tape/binance/um")
        .join(symbol)
        .join(format!("{}.jsonl.zst", date.format("%Y-%m-%d")))
}

/// Statistics for one built day.
#[derive(Debug, Default)]
pub struct TapeStats {
    pub per_product: BTreeMap<String, DecodeStats>,
    pub events: u64,
    pub bytes: u64,
    /// Minutes of the day for which a 1m kline closed, as `open_time` ms.
    pub kline_minutes: Vec<i64>,
    pub first_ts: Option<i64>,
    pub last_ts: Option<i64>,
    pub elapsed_ms: u128,
}

/// Builds one day's tape from the raw objects that exist.
///
/// `extra` carries events that do not come from an archive object (funding).
#[allow(clippy::too_many_arguments)]
pub fn build_day(root: &Path, symbol: &str, interval: &str, date: NaiveDate, products: &[Product], extra: Vec<Event>) -> Result<TapeStats> {
    let started = Instant::now();
    let mut decoders: Vec<(Product, Decoder)> = Vec::new();
    for product in products {
        let path = product.raw_path(root, symbol, date, interval);
        if path.exists() {
            decoders.push((*product, spawn(*product, &path, 4096)?));
        }
    }

    let mut sources: Vec<Reorder> = Vec::new();
    for (_, decoder) in decoders {
        sources.push(Reorder::channel(decoder));
    }
    if !extra.is_empty() {
        sources.push(Reorder::from_events(extra));
    }

    let mut heap: BinaryHeap<Reverse<Item>> = BinaryHeap::new();
    for (index, source) in sources.iter_mut().enumerate() {
        if let Some(event) = source.next()? {
            heap.push(Reverse(Item {
                key: event.key(),
                index,
                event,
            }));
        }
    }

    let path = tape_path(root, symbol, date);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(&path)?;
    let mut encoder = zstd::Encoder::new(BufWriter::with_capacity(1 << 20, file), 3)?;

    let mut line = String::with_capacity(512);
    let mut stats = TapeStats::default();

    while let Some(Reverse(item)) = heap.pop() {
        line.clear();
        item.event.write_json(symbol, &mut line);
        line.push('\n');
        encoder.write_all(line.as_bytes())?;

        stats.events += 1;
        stats.first_ts.get_or_insert(item.event.ts);
        stats.last_ts = Some(item.event.ts);
        if let Payload::Kline { .. } = item.event.payload {
            stats.kline_minutes.push(item.event.id as i64);
        }

        if let Some(event) = sources[item.index].next()? {
            heap.push(Reverse(Item {
                key: event.key(),
                index: item.index,
                event,
            }));
        }
    }

    encoder.finish()?;
    for source in sources {
        if let Some((product, decoder)) = source.into_decoder() {
            let product_stats = decoder.finish()?;
            stats.per_product.insert(product.as_str().to_owned(), product_stats);
        }
    }
    stats.kline_minutes.sort_unstable();
    stats.bytes = std::fs::metadata(&path)?.len();
    stats.elapsed_ms = started.elapsed().as_millis();
    Ok(stats)
}

struct Item {
    key: (i64, u8, u64),
    index: usize,
    event: Event,
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.index == other.index
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key).then(self.index.cmp(&other.index))
    }
}

/// A source that tolerates small timestamp inversions in the archive.
///
/// Files are essentially time-ordered; a handful of out-of-order rows do exist.
/// The buffer keeps a window sorted, so the tape is monotone by construction,
/// while a violation wider than the window is a hard error rather than a silent
/// reordering of history.
struct Reorder {
    inner: Inner,
    buffer: BinaryHeap<Reverse<Buffered>>,
    sequence: u64,
    window: usize,
    frontier: i64,
}

enum Inner {
    Channel(Decoder),
    Memory(VecDeque<Event>),
    Empty,
}

struct Buffered {
    key: (i64, u8, u64),
    sequence: u64,
    event: Event,
}

impl PartialEq for Buffered {
    fn eq(&self, other: &Self) -> bool {
        (self.key, self.sequence) == (other.key, other.sequence)
    }
}
impl Eq for Buffered {}
impl PartialOrd for Buffered {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Buffered {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key).then(self.sequence.cmp(&other.sequence))
    }
}

impl Reorder {
    fn new() -> Self {
        Self {
            inner: Inner::Empty,
            buffer: BinaryHeap::new(),
            sequence: 0,
            window: 8192,
            frontier: i64::MIN,
        }
    }

    fn channel(decoder: Decoder) -> Self {
        Self {
            inner: Inner::Channel(decoder),
            ..Self::new()
        }
    }

    fn into_decoder(self) -> Option<(Product, Decoder)> {
        match self.inner {
            Inner::Channel(decoder) => Some((decoder.product, decoder)),
            _ => None,
        }
    }

    fn from_events(events: Vec<Event>) -> Self {
        Self {
            inner: Inner::Memory(events.into()),
            ..Self::new()
        }
    }

    fn pull(&mut self) -> Result<Option<Event>> {
        match &mut self.inner {
            Inner::Channel(decoder) => match decoder.rx.recv() {
                Ok(Ok(event)) => Ok(Some(event)),
                Ok(Err(error)) => Err(error),
                Err(_) => Ok(None),
            },
            Inner::Memory(events) => Ok(events.pop_front()),
            Inner::Empty => Ok(None),
        }
    }

    fn next(&mut self) -> Result<Option<Event>> {
        while self.buffer.len() < self.window {
            match self.pull()? {
                Some(event) => {
                    self.sequence += 1;
                    self.buffer.push(Reverse(Buffered {
                        key: event.key(),
                        sequence: self.sequence,
                        event,
                    }));
                }
                None => break,
            }
        }
        match self.buffer.pop() {
            Some(Reverse(buffered)) => {
                if self.frontier != i64::MIN && buffered.key.0 < self.frontier.saturating_sub(self.window as i64) {
                    return Err(Error::OutOfOrder {
                        ts: buffered.key.0,
                        frontier: self.frontier,
                    });
                }
                self.frontier = self.frontier.max(buffered.key.0);
                Ok(Some(buffered.event))
            }
            None => Ok(None),
        }
    }
}

/// Reads the funding series for the days in `[from, to]`.
pub fn funding_for(path: &Path, from: i64, to: i64) -> Result<Vec<Event>> {
    let text = std::fs::read_to_string(path)?;
    let mut events = Vec::new();
    for line in text.lines().skip(1) {
        let mut fields = line.split(',');
        let (Some(ts), Some(rate)) = (fields.next(), fields.next()) else {
            continue;
        };
        let Ok(ts) = ts.trim().parse::<i64>() else { continue };
        let Ok(rate) = rate.trim().parse::<f64>() else { continue };
        if ts < from || ts >= to {
            continue;
        }
        events.push(Event {
            ts,
            kind: Kind::Funding,
            id: ts as u64,
            payload: Payload::Funding { rate, stamp: ts },
        });
    }
    events.sort_by_key(|event| event.key());
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reorder_sorts_a_small_inversion() {
        let mut source = Reorder::from_events(vec![
            Event {
                ts: 10,
                kind: Kind::Trade,
                id: 1,
                payload: Payload::Trade {
                    price: 1.0,
                    size: 1.0,
                    side: "buy",
                },
            },
            Event {
                ts: 8,
                kind: Kind::Trade,
                id: 2,
                payload: Payload::Trade {
                    price: 1.0,
                    size: 1.0,
                    side: "buy",
                },
            },
            Event {
                ts: 9,
                kind: Kind::Trade,
                id: 3,
                payload: Payload::Trade {
                    price: 1.0,
                    size: 1.0,
                    side: "buy",
                },
            },
        ]);
        let mut seen = Vec::new();
        while let Some(event) = source.next().unwrap() {
            seen.push(event.ts);
        }
        assert_eq!(seen, vec![8, 9, 10]);
    }

    #[test]
    fn funding_is_filtered_to_the_window() {
        let dir = std::env::temp_dir().join(format!("funding-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("BTCUSDT.csv");
        std::fs::write(&path, "ts_ms,rate\n100,0.0001\n200,-0.0002\n300,0.0003\n").unwrap();

        let events = funding_for(&path, 150, 300).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ts, 200);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
