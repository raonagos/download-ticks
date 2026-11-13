//! Building a dataset: raw objects in, tape plus manifest out.
//!
//! The days of a range are independent — one tape file each — so they are built
//! on several threads: a day that is compressing no longer leaves the other
//! cores waiting. A day still decodes its own objects on its own threads and
//! sorts them in memory; what changes is how many days are in flight.
//!
//! The aggregation is done afterwards, in day order, so the manifest and the
//! `dataset_id` are the same whether the build ran with one worker or eight.

use std::path::Path;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use chrono::{DateTime, NaiveDate, Utc};

use crate::archive::{Product, Range, sha256_file};
use crate::errors::Result;
use crate::manifest::{Gap, Manifest, ObjectEntry, RangeJson, SCHEMA, Tool, Totals, kline_gaps};
use crate::parallel;
use crate::tape::{FundingSeries, TapeStats, build_day};

/// What a build run produced, for the report.
#[derive(Debug)]
pub struct BuildReport {
    pub days: usize,
    pub events: u64,
    pub tape_bytes: u64,
    pub manifest: Manifest,
    pub elapsed_ms: u128,
    /// The memory the workers were allowed to share, in bytes.
    pub memory_budget: u64,
}

/// Memory per byte of archive, deliberately pessimistic.
///
/// A day is decoded and sorted in RAM before it is written: ~80 bytes per
/// event, and the archive carries ~9 bytes per event. 10 leaves room for the
/// sort buffer, the decoder channels and the encoder. Overestimating costs a
/// little parallelism; underestimating gets the machine killed.
const MEMORY_PER_ARCHIVE_BYTE: u64 = 10;

/// The bytes the workers of one build may hold in flight.
struct MemoryBudget {
    free: Mutex<u64>,
    ready: Condvar,
    limit: u64,
}

impl MemoryBudget {
    fn new(limit: u64) -> Self {
        Self {
            free: Mutex::new(limit),
            ready: Condvar::new(),
            limit,
        }
    }

    /// Waits for the estimate to fit, then reserves it.
    ///
    /// A day whose estimate exceeds the whole budget still runs: it runs alone.
    /// Otherwise an oversized day would never be admitted.
    fn reserve(&self, bytes: u64) -> Reservation<'_> {
        let bytes = bytes.clamp(1, self.limit);
        let mut free = self.free.lock().unwrap();
        while *free < bytes {
            free = self.ready.wait(free).unwrap();
        }
        *free -= bytes;
        Reservation { budget: self, bytes }
    }
}

/// A reservation, released when the day it paid for is built.
struct Reservation<'a> {
    budget: &'a MemoryBudget,
    bytes: u64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut free = self.budget.free.lock().unwrap();
        *free += self.bytes;
        self.budget.ready.notify_all();
    }
}

/// One day, built and ready to be folded into the manifest.
struct DayOutcome {
    date: NaiveDate,
    stats: TapeStats,
    objects: Vec<ObjectEntry>,
    gaps: Vec<Gap>,
    funding_rows: u64,
    funding_min: Option<i64>,
    funding_max: Option<i64>,
}

/// Builds every day of the range and writes `manifest.json`.
pub fn build(root: &Path, symbol: &str, interval: &str, range: Range, products: &[Product], tool_commit: &str, jobs: usize) -> Result<BuildReport> {
    let started = Instant::now();
    let funding_path = root.join("funding/binance/um").join(format!("{symbol}.csv"));
    let funding = if funding_path.exists() {
        FundingSeries::read(&funding_path)?
    } else {
        FundingSeries::default()
    };
    let days: Vec<NaiveDate> = range.iter().collect();
    let budget = MemoryBudget::new(parallel::memory_budget());

    let outcomes = parallel::map(
        &days,
        jobs,
        |_, date| build_one(root, symbol, interval, *date, products, &funding, &budget),
        |_, outcome| {
            if let Ok(day) = outcome {
                println!(
                    "{}  events {:>10}  tape {:>10}  {} ms",
                    day.date,
                    day.stats.events,
                    human_bytes(day.stats.bytes),
                    day.stats.elapsed_ms
                );
            }
        },
    )?;

    let mut objects = Vec::new();
    let mut gaps = Vec::new();
    let mut funding_rows = 0u64;
    let mut funding_min: Option<i64> = None;
    let mut funding_max: Option<i64> = None;
    let mut report = BuildReport {
        days: 0,
        events: 0,
        tape_bytes: 0,
        manifest: Manifest {
            schema: SCHEMA,
            dataset_id: String::new(),
            created_utc: String::new(),
            tool: Tool {
                name: String::new(),
                version: String::new(),
                commit: String::new(),
            },
            venue: String::new(),
            market: String::new(),
            symbol: String::new(),
            range: RangeJson {
                from: String::new(),
                to: String::new(),
            },
            objects: Vec::new(),
            gaps: Vec::new(),
            totals: Totals::default(),
        },
        elapsed_ms: 0,
        memory_budget: budget.limit,
    };

    for day in outcomes {
        report.days += 1;
        report.events += day.stats.events;
        report.tape_bytes += day.stats.bytes;
        objects.extend(day.objects);
        gaps.extend(day.gaps);
        funding_rows += day.funding_rows;
        funding_min = match (funding_min, day.funding_min) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        funding_max = match (funding_max, day.funding_max) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
    }

    // Funding is part of the dataset, not a side file: it enters the identity.
    if funding_rows > 0 {
        objects.push(ObjectEntry {
            kind: "funding".to_owned(),
            interval: None,
            path: relative(root, &funding_path),
            sha256: sha256_file(&funding_path)?,
            rows: funding_rows,
            min_ts: funding_min.unwrap_or(0),
            max_ts: funding_max.unwrap_or(0),
            bytes: std::fs::metadata(&funding_path)?.len(),
            source: "rest".to_owned(),
            status: "ok".to_owned(),
        });
    }

    objects.sort_by(|a, b| (a.kind.as_str(), a.min_ts).cmp(&(b.kind.as_str(), b.min_ts)));
    gaps.sort_by(|a, b| (a.kind.as_str(), a.from.as_str()).cmp(&(b.kind.as_str(), b.from.as_str())));

    let totals = Totals {
        objects: objects.len(),
        rows: objects.iter().map(|object| object.rows).sum(),
        bytes: objects.iter().map(|object| object.bytes).sum(),
        gaps: gaps.len(),
        duplicates: 0,
    };

    let mut manifest = Manifest {
        schema: SCHEMA,
        dataset_id: String::new(),
        created_utc: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        tool: Tool {
            name: "download-ticks".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            commit: tool_commit.to_owned(),
        },
        venue: "binance".to_owned(),
        market: "um".to_owned(),
        symbol: symbol.to_owned(),
        range: RangeJson {
            from: range.from.to_string(),
            to: range.to.to_string(),
        },
        objects,
        gaps,
        totals,
    };
    manifest.dataset_id = manifest.recompute_id()?;
    manifest.write(&Manifest::path(root))?;

    report.manifest = manifest;
    report.elapsed_ms = started.elapsed().as_millis();
    Ok(report)
}

/// Builds one day: its tape file, its objects and its gaps.
fn build_one(
    root: &Path,
    symbol: &str,
    interval: &str,
    date: NaiveDate,
    products: &[Product],
    funding: &FundingSeries,
    budget: &MemoryBudget,
) -> Result<DayOutcome> {
    let _reservation = budget.reserve(estimate_day(root, symbol, interval, date, products));
    let day_start = day_start_ms(date);
    let day_end = day_start + 86_400_000;
    let extra = funding.for_day(day_start, day_end);

    let mut funding_rows = 0u64;
    let mut funding_min: Option<i64> = None;
    let mut funding_max: Option<i64> = None;
    for stamp in &extra {
        funding_rows += 1;
        funding_min = Some(funding_min.map_or(stamp.ts, |min| min.min(stamp.ts)));
        funding_max = Some(funding_max.map_or(stamp.ts, |max| max.max(stamp.ts)));
    }

    let stats = build_day(root, symbol, interval, date, products, extra)?;
    let mut objects = Vec::new();
    let mut gaps = Vec::new();

    for product in products {
        let raw = product.raw_path(root, symbol, date, interval);
        if !raw.exists() {
            gaps.push(Gap {
                kind: product.as_str().to_owned(),
                from: iso(day_start),
                to: iso(day_end),
                missing: 1,
                reason: "archive missing".to_owned(),
            });
            continue;
        }
        let decoded = stats.per_product.get(product.as_str()).copied().unwrap_or_default();
        if decoded.rows == 0 {
            gaps.push(Gap {
                kind: product.as_str().to_owned(),
                from: iso(day_start),
                to: iso(day_end),
                missing: 1,
                reason: "object decoded to zero rows".to_owned(),
            });
            continue;
        }
        objects.push(ObjectEntry {
            kind: product.as_str().to_owned(),
            interval: (*product == Product::Klines).then(|| interval.to_owned()),
            path: relative(root, &raw),
            sha256: sha256_file(&raw)?,
            rows: decoded.rows,
            min_ts: decoded.min_ts.unwrap_or(0),
            max_ts: decoded.max_ts.unwrap_or(0),
            bytes: std::fs::metadata(&raw)?.len(),
            source: "vision".to_owned(),
            status: "ok".to_owned(),
        });

        if *product == Product::Klines {
            for (start, missing) in kline_gaps(day_start, &stats.kline_minutes) {
                gaps.push(Gap {
                    kind: "klines".to_owned(),
                    from: iso(start),
                    to: iso(start + missing * 60_000),
                    missing: missing as u64,
                    reason: "minute missing from the archive".to_owned(),
                });
            }
        }
    }

    Ok(DayOutcome {
        date,
        stats,
        objects,
        gaps,
        funding_rows,
        funding_min,
        funding_max,
    })
}

/// A pessimistic memory estimate for one day, from the size of its objects.
fn estimate_day(root: &Path, symbol: &str, interval: &str, date: NaiveDate, products: &[Product]) -> u64 {
    let archived: u64 = products
        .iter()
        .filter_map(|product| std::fs::metadata(product.raw_path(root, symbol, date, interval)).ok())
        .map(|metadata| metadata.len())
        .sum();
    estimate_from(archived)
}

/// The floor keeps a day of tiny objects from being counted as free.
fn estimate_from(archived: u64) -> u64 {
    archived.saturating_mul(MEMORY_PER_ARCHIVE_BYTE).max(64 << 20)
}

/// Midnight UTC of a day, in milliseconds.
pub fn day_start_ms(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0).expect("valid midnight").and_utc().timestamp_millis()
}

pub fn iso(ts_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .map(|moment| moment.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).to_string_lossy().replace('\\', "/")
}

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tape::tape_path;
    use crate::testing::{scratch, write_klines};
    use std::fs;

    #[test]
    fn a_parallel_build_reproduces_the_sequential_manifest() {
        let sequential = scratch("build-seq");
        let concurrent = scratch("build-par");
        let range = Range::parse("2024-01-15", "2024-01-16").unwrap();
        for root in [&sequential, &concurrent] {
            for date in range.iter() {
                write_klines(root, date, 3);
            }
        }
        let products = [Product::Klines];

        let one = build(&sequential, "BTCUSDT", "1m", range, &products, "test", 1).unwrap();
        let many = build(&concurrent, "BTCUSDT", "1m", range, &products, "test", 4).unwrap();

        assert_eq!(one.manifest.dataset_id, many.manifest.dataset_id);
        assert_eq!(one.manifest.totals.rows, 6);
        assert_eq!(many.manifest.totals.rows, 6);
        for date in range.iter() {
            let left = fs::read(tape_path(&sequential, "BTCUSDT", date)).unwrap();
            let right = fs::read(tape_path(&concurrent, "BTCUSDT", date)).unwrap();
            assert_eq!(left, right, "tape bytes differ on {date}");
        }
        assert!(many.memory_budget >= 4 << 30, "a build always has a budget");

        fs::remove_dir_all(sequential).unwrap();
        fs::remove_dir_all(concurrent).unwrap();
    }

    #[test]
    fn a_day_without_an_object_is_a_gap_in_a_parallel_build() {
        let root = scratch("build-gap");
        let range = Range::parse("2024-01-15", "2024-01-16").unwrap();
        write_klines(&root, range.from, 2);
        let products = [Product::Klines];

        let report = build(&root, "BTCUSDT", "1m", range, &products, "test", 2).unwrap();

        assert_eq!(report.days, 2);
        assert_eq!(report.manifest.totals.rows, 2);
        let missing: Vec<_> = report.manifest.gaps.iter().filter(|gap| gap.reason == "archive missing").collect();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].kind, "klines");
        assert!(missing[0].from.starts_with("2024-01-16"));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn the_memory_estimate_is_ten_bytes_of_ram_per_archive_byte() {
        assert_eq!(estimate_from(0), 64 << 20, "an absent day still reserves a floor");
        assert_eq!(estimate_from(100 << 20), 1000 << 20);
        assert_eq!(estimate_from(1 << 20), 64 << 20, "a small day keeps the floor");
    }
}
