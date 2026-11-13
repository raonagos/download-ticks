//! `verify`: the nine checks of data-contract §7, streamed.
//!
//! A dataset either passes all of them or it is not a dataset. Every failure is
//! printed and the command exits non-zero; nothing is repaired here.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde_json::Value;

use crate::archive::{Product, Range, sha256_file};
use crate::build::day_start_ms;
use crate::decode::decode_with;
use crate::errors::Result;
use crate::manifest::{Manifest, kline_gaps};
use crate::tape::tape_path;

/// The outcome of a verification run.
#[derive(Debug, Default)]
pub struct Verification {
    pub failures: Vec<String>,
    pub checks: Vec<(String, bool, String)>,
}

impl Verification {
    fn record(&mut self, name: &str, ok: bool, detail: String) {
        if !ok {
            self.failures.push(format!("{name}: {detail}"));
        }
        self.checks.push((name.to_owned(), ok, detail));
    }
}

/// Verifies a dataset rooted at `root`.
pub fn verify(root: &Path, deep: bool) -> Result<Verification> {
    let mut report = Verification::default();
    let mut manifest = Manifest::read(&Manifest::path(root))?;

    // 9. dataset_id recomputes.
    let recomputed = manifest.recompute_id()?;
    report.record(
        "dataset_id",
        recomputed == manifest.dataset_id,
        format!("{} (manifest says {})", recomputed, manifest.dataset_id),
    );

    // 1. every object is present and matches its checksum.
    let mut objects_ok = true;
    let mut objects_detail = format!("{} objects", manifest.objects.len());
    for object in &manifest.objects {
        let path = root.join(&object.path);
        if !path.exists() {
            objects_ok = false;
            objects_detail = format!("{} is missing", object.path);
            break;
        }
        let actual = sha256_file(&path)?;
        if actual != object.sha256 {
            objects_ok = false;
            objects_detail = format!("{} hashes to {actual}, manifest says {}", object.path, object.sha256);
            break;
        }
    }
    report.record("objects/checksums", objects_ok, objects_detail);

    // 2 and 8. Row counts: tape against the manifest, and (deep) raw against it.
    let range = Range::parse(&manifest.range.from, &manifest.range.to)?;
    let mut tape_counts: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut tape_objects = 0usize;
    let mut tape_ok = true;
    let mut tape_detail = String::new();
    let mut kline_minutes: std::collections::BTreeMap<chrono::NaiveDate, Vec<i64>> = std::collections::BTreeMap::new();

    for day in range.iter() {
        let path = tape_path(root, &manifest.symbol, day);
        if !path.exists() {
            tape_ok = false;
            tape_detail = format!("tape for {day} is missing");
            break;
        }
        tape_objects += 1;
        match scan_tape(&path, &mut tape_counts) {
            Ok(minutes) => {
                kline_minutes.insert(day, minutes);
            }
            Err(error) => {
                tape_ok = false;
                tape_detail = format!("tape for {day}: {error}");
                break;
            }
        }
    }
    report.record(
        "tape/structure",
        tape_ok,
        if tape_ok {
            format!("{tape_objects} day files, {} events", tape_counts.values().sum::<u64>())
        } else {
            tape_detail
        },
    );

    let mut counts_ok = true;
    let mut counts_detail = String::new();
    for (kind, expected) in manifest_rows_by_kind(&manifest) {
        let actual = tape_counts.get(&kind).copied().unwrap_or(0);
        if actual != expected {
            counts_ok = false;
            counts_detail = format!("{kind}: tape has {actual}, manifest says {expected}");
            break;
        }
    }
    report.record(
        "tape/row-counts",
        counts_ok,
        if counts_ok { "match the manifest".to_owned() } else { counts_detail },
    );

    // 8 (deep). Decode the raw objects again and compare to the manifest.
    if deep {
        let mut deep_ok = true;
        let mut deep_detail = String::new();
        for object in &manifest.objects {
            if object.kind == "funding" {
                // Funding is a REST series, not a ZIP: its rows are checked
                // through the tape count and its bytes through the checksum.
                continue;
            }
            let Some(product) = Product::parse(&object.kind) else {
                deep_ok = false;
                deep_detail = format!("unknown kind {}", object.kind);
                break;
            };
            let stats = decode_with(product, &root.join(&object.path), |_| true)?;
            if stats.rows != object.rows {
                deep_ok = false;
                deep_detail = format!("{}: decoded {} rows, manifest says {}", object.path, stats.rows, object.rows);
                break;
            }
        }
        report.record(
            "raw/row-counts",
            deep_ok,
            if deep_ok { "decoded counts match".to_owned() } else { deep_detail },
        );
    }

    // 4. klines sit on the 1m grid and every hole is listed, nothing is filled.
    let mut listed: Vec<(i64, i64)> = Vec::new();
    for gap in &manifest.gaps {
        if gap.kind != "klines" {
            continue;
        }
        let Some(from) = parse_iso(&gap.from) else {
            listed.push((i64::MIN, 0));
            continue;
        };
        listed.push((from, gap.missing as i64));
    }
    listed.sort_unstable();

    let mut found: Vec<(i64, i64)> = Vec::new();
    for (day, minutes) in &kline_minutes {
        found.extend(kline_gaps(day_start_ms(*day), minutes));
    }
    found.sort_unstable();

    let grid_ok = listed == found;
    report.record(
        "klines/grid",
        grid_ok,
        if grid_ok {
            format!("{} gap run(s) listed, none filled", found.len())
        } else {
            format!("listed {listed:?} but the tape has {found:?}")
        },
    );

    // Every "archive missing" gap really is a day without an object.
    let mut missing_ok = true;
    let mut missing_detail = String::from("no archive gaps");
    let mut missing_gaps = 0usize;
    for gap in &manifest.gaps {
        if gap.reason != "archive missing" {
            continue;
        }
        missing_gaps += 1;
        let Some(day) = parse_iso(&gap.from).and_then(chrono::DateTime::from_timestamp_millis) else {
            missing_ok = false;
            missing_detail = format!("unparseable gap {}", gap.from);
            break;
        };
        let date = day.date_naive();
        let Some(product) = Product::parse(&gap.kind) else {
            missing_ok = false;
            missing_detail = format!("gap for unknown kind {}", gap.kind);
            break;
        };
        let mut path = root.join("raw/binance/um").join(&manifest.symbol).join(match product {
            Product::Klines => "klines",
            Product::AggTrades => "aggTrades",
            Product::BookTicker => "bookTicker",
            Product::BookDepth => "bookDepth",
        });
        if product == Product::Klines {
            path = path.join("1m");
        }
        let name = product.file_name(&manifest.symbol, date, "1m");
        if path.join(&name).exists() {
            missing_ok = false;
            missing_detail = format!("{} is listed as missing but {} exists", gap.kind, name);
            break;
        }
    }
    if missing_ok && missing_gaps > 0 {
        missing_detail = format!("{missing_gaps} archive gap(s) confirmed absent");
    }
    report.record("gaps/archive-missing", missing_ok, missing_detail);

    // 7. funding stamps are unique and ordered (checked while scanning the tape).
    report.record(
        "funding/order",
        !report.failures.iter().any(|failure| failure.contains("funding")),
        "unique and ordered".to_owned(),
    );

    Ok(report)
}

fn parse_iso(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value).ok().map(|moment| moment.timestamp_millis())
}

fn manifest_rows_by_kind(manifest: &Manifest) -> std::collections::BTreeMap<String, u64> {
    let mut rows: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for object in &manifest.objects {
        let kind = Product::parse(&object.kind).map(Product::event_kind).unwrap_or(object.kind.as_str());
        *rows.entry(kind.to_owned()).or_default() += object.rows;
    }
    rows
}

/// Streams one day's tape and checks the per-event invariants.
fn scan_tape(path: &Path, counts: &mut std::collections::BTreeMap<String, u64>) -> Result<Vec<i64>> {
    let file = File::open(path)?;
    let decoder = zstd::Decoder::new(BufReader::with_capacity(1 << 20, file))?;
    let decoder = BufReader::new(decoder);
    let mut last_ts = i64::MIN;
    let mut last_kind = 0u8;
    // The tape is sorted by (ts, kind, id), not by id: ids are collected and
    // checked for uniqueness after the scan, never for monotonicity.
    let mut ids: std::collections::BTreeMap<String, Vec<u64>> = std::collections::BTreeMap::new();
    let mut kline_minutes: Vec<i64> = Vec::new();

    for line in decoder.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)?;
        let ts = value["ts"].as_i64().unwrap_or(0);
        let kind = value["kind"].as_str().unwrap_or("");
        let id = value["id"].as_u64().unwrap_or(0);
        let rank = kind_rank(kind);

        if ts < last_ts {
            return Err(crate::errors::Error::Verify(format!("{kind} at {ts} comes after {last_ts}")));
        }
        if ts == last_ts && rank < last_kind {
            return Err(crate::errors::Error::Verify(format!("{kind} out of kind order at {ts}")));
        }
        last_ts = ts;
        last_kind = rank;

        ids.entry(kind.to_owned()).or_default().push(id);

        match kind {
            "trade" => {
                if !positive(&value, "price") || !positive(&value, "size") {
                    return Err(crate::errors::Error::Verify("trade with non-positive price/size".to_owned()));
                }
                if !matches!(value["side"].as_str(), Some("buy" | "sell")) {
                    return Err(crate::errors::Error::Verify("trade with an invalid side".to_owned()));
                }
            }
            "book_ticker" => {
                let (bid, ask) = (value["bid"].as_f64().unwrap_or(0.0), value["ask"].as_f64().unwrap_or(0.0));
                if !(bid > 0.0 && ask > 0.0 && bid < ask) || !positive(&value, "bid_size") || !positive(&value, "ask_size") {
                    return Err(crate::errors::Error::Verify(format!("crossed or empty book at {ts}")));
                }
            }
            "book_depth" => {
                let bands = value["bands"].as_array().map(Vec::as_slice).unwrap_or_default();
                if bands.len() != 10 {
                    return Err(crate::errors::Error::Verify(format!("depth snapshot with {} bands at {ts}", bands.len())));
                }
                for band in bands {
                    if band["depth"].as_f64().unwrap_or(0.0) <= 0.0 || band["notional"].as_f64().unwrap_or(0.0) <= 0.0 {
                        return Err(crate::errors::Error::Verify(format!("depth band with non-positive values at {ts}")));
                    }
                }
            }
            "kline" => {
                if !id.is_multiple_of(60_000) {
                    return Err(crate::errors::Error::Verify(format!("kline off the 1m grid at {ts}")));
                }
                kline_minutes.push(id as i64);
            }
            "funding" => {
                if value["stamp"].as_i64() != Some(ts) {
                    return Err(crate::errors::Error::Verify(format!("funding stamp mismatch at {ts}")));
                }
            }
            _ => return Err(crate::errors::Error::Verify(format!("unknown kind {kind}"))),
        }

        *counts.entry(kind.to_owned()).or_default() += 1;
    }

    for (kind, mut kind_ids) in ids {
        kind_ids.sort_unstable();
        if let Some(pair) = kind_ids.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(crate::errors::Error::Verify(format!("duplicate {kind} id {}", pair[0])));
        }
    }

    Ok(kline_minutes)
}

fn positive(value: &Value, field: &str) -> bool {
    value[field].as_f64().map(|number| number > 0.0).unwrap_or(false)
}

fn kind_rank(kind: &str) -> u8 {
    match kind {
        "book_depth" => 0,
        "book_ticker" => 1,
        "funding" => 2,
        "kline" => 3,
        "trade" => 4,
        _ => 5,
    }
}
