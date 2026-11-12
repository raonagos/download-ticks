//! Building a dataset: raw objects in, tape plus manifest out.

use std::path::Path;
use std::time::Instant;

use chrono::{DateTime, NaiveDate, Utc};

use crate::archive::{Product, Range, sha256_file};
use crate::errors::Result;
use crate::manifest::{Gap, Manifest, ObjectEntry, RangeJson, SCHEMA, Tool, Totals, kline_gaps};
use crate::tape::{build_day, funding_for};

/// What a build run produced, for the report.
#[derive(Debug)]
pub struct BuildReport {
    pub days: usize,
    pub events: u64,
    pub tape_bytes: u64,
    pub manifest: Manifest,
    pub elapsed_ms: u128,
}

/// Builds every day of the range and writes `manifest.json`.
pub fn build(root: &Path, symbol: &str, interval: &str, range: Range, products: &[Product], tool_commit: &str) -> Result<BuildReport> {
    let started = Instant::now();
    let funding_path = root.join("funding/binance/um").join(format!("{symbol}.csv"));
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
    };

    for date in range.iter() {
        let day_start = day_start_ms(date);
        let day_end = day_start + 86_400_000;
        let extra = if funding_path.exists() {
            funding_for(&funding_path, day_start, day_end)?
        } else {
            Vec::new()
        };

        for stamp in &extra {
            funding_rows += 1;
            funding_min = Some(funding_min.map_or(stamp.ts, |min| min.min(stamp.ts)));
            funding_max = Some(funding_max.map_or(stamp.ts, |max| max.max(stamp.ts)));
        }
        let stats = build_day(root, symbol, interval, date, products, extra)?;
        report.days += 1;
        report.events += stats.events;
        report.tape_bytes += stats.bytes;

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

        println!(
            "{}  events {:>10}  tape {:>10}  {} ms",
            date,
            stats.events,
            human_bytes(stats.bytes),
            stats.elapsed_ms
        );
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
