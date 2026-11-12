//! Building the canonical tape: one day decoded, sorted, and written.
//!
//! The archive is **not** time-ordered. A 2024-01-02 `bookTicker` file
//! interleaves two `update_id` streams, one of which carries the last seconds
//! of the day, 361k lines before the end of the first stream; 180,805 adjacent
//! timestamp inversions in 18.5M rows. A reorder window cannot absorb that, so
//! a day is decoded in parallel and sorted in memory before it is written.
//!
//! Sorting a day costs roughly 1.5 GiB for 20M events. That is a conscious
//! trade: the tape must be sorted by `(ts, kind, id)` for a replay to be
//! meaningful, and an external sort would trade the memory for a day of disk
//! I/O per day of data.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::NaiveDate;

use crate::archive::Product;
use crate::decode::{DecodeStats, Event, Kind, Payload, spawn};
use crate::errors::{Error, Result};

/// Refuse a day beyond this many events rather than exhausting memory.
const MAX_EVENTS_PER_DAY: usize = 40_000_000;

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
    let mut decoders = Vec::new();
    for product in products {
        let path = product.raw_path(root, symbol, date, interval);
        if path.exists() {
            decoders.push((*product, spawn(*product, &path, 8192)?));
        }
    }

    // Drain every decoder at once: the merge order is decided by the sort, so
    // the reader order does not matter, and draining keeps the threads busy.
    let mut events: Vec<Event> = Vec::with_capacity(1 << 20);
    events.extend(extra);
    let mut per_product = Vec::new();
    for (product, decoder) in decoders {
        while let Ok(event) = decoder.rx.recv() {
            let event = event?;
            if events.len() >= MAX_EVENTS_PER_DAY {
                return Err(Error::DayTooLarge(MAX_EVENTS_PER_DAY));
            }
            events.push(event);
        }
        per_product.push((product, decoder.finish()?));
    }

    events.sort_by_key(|event| event.key());

    let path = tape_path(root, symbol, date);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(&path)?;
    let mut encoder = zstd::Encoder::new(BufWriter::with_capacity(1 << 20, file), 3)?;

    let mut line = String::with_capacity(512);
    let mut stats = TapeStats::default();
    for event in &events {
        line.clear();
        event.write_json(symbol, &mut line);
        line.push('\n');
        encoder.write_all(line.as_bytes())?;
        stats.events += 1;
        stats.first_ts.get_or_insert(event.ts);
        stats.last_ts = Some(event.ts);
        if let Payload::Kline { .. } = event.payload {
            stats.kline_minutes.push(event.id as i64);
        }
    }
    encoder.finish()?;

    for (product, product_stats) in per_product {
        stats.per_product.insert(product.as_str().to_owned(), product_stats);
    }
    stats.kline_minutes.sort_unstable();
    stats.bytes = std::fs::metadata(&path)?.len();
    stats.elapsed_ms = started.elapsed().as_millis();
    drop(events);
    Ok(stats)
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
