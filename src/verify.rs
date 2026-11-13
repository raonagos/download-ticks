//! `verify`: the nine checks of data-contract §7, streamed.
//!
//! A dataset either passes all of them or it is not a dataset. Every failure is
//! printed and the command exits non-zero; nothing is repaired here.
//!
//! The expensive parts — hashing every object, scanning every day's tape,
//! decoding every raw object again — are independent per item, so they run on
//! several threads. The checks are still decided in a fixed order: the first
//! failure reported is the first one in the manifest, or the first day of the
//! range, not whichever finished first.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::NaiveDate;
use serde_json::Value;

use crate::archive::{Product, Range, sha256_file};
use crate::build::day_start_ms;
use crate::decode::decode_with;
use crate::errors::Result;
use crate::manifest::{Manifest, ObjectEntry, kline_gaps};
use crate::parallel;
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

/// One day's tape, scanned.
struct DayTape {
    counts: BTreeMap<String, u64>,
    minutes: Vec<i64>,
}

/// What scanning a day produced: a tape, nothing, or a broken tape.
///
/// A corrupt tape is a failed check, not an aborted run: the remaining checks
/// still run and the report says which day broke.
enum TapeScan {
    Missing,
    Failed(String),
    Scanned(DayTape),
}

/// Verifies a dataset rooted at `root`, with `jobs` workers.
pub fn verify(root: &Path, deep: bool, jobs: usize) -> Result<Verification> {
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
    let objects = parallel::map(&manifest.objects, jobs, |_, object| hash_object(root, object), |_, _| {})?;
    let objects_ok = objects.iter().all(Option::is_none);
    let objects_detail = match objects.iter().flatten().next() {
        Some(detail) => detail.clone(),
        None => format!("{} objects", manifest.objects.len()),
    };
    report.record("objects/checksums", objects_ok, objects_detail);

    // 2 and 8. Row counts: tape against the manifest, and (deep) raw against it.
    let range = Range::parse(&manifest.range.from, &manifest.range.to)?;
    let days: Vec<NaiveDate> = range.iter().collect();
    let scans = parallel::map(&days, jobs, |_, day| scan_day(root, &manifest.symbol, *day), |_, _| {})?;

    let mut tape_counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut kline_minutes: BTreeMap<NaiveDate, Vec<i64>> = BTreeMap::new();
    let mut tape_objects = 0usize;
    let mut tape_ok = true;
    let mut tape_detail = String::new();
    for (day, scan) in days.iter().zip(&scans) {
        match scan {
            TapeScan::Missing => {
                tape_ok = false;
                tape_detail = format!("tape for {day} is missing");
                break;
            }
            TapeScan::Failed(detail) => {
                tape_ok = false;
                tape_detail = format!("tape for {day}: {detail}");
                break;
            }
            TapeScan::Scanned(tape) => {
                tape_objects += 1;
                kline_minutes.insert(*day, tape.minutes.clone());
                for (kind, count) in &tape.counts {
                    *tape_counts.entry(kind.clone()).or_default() += count;
                }
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
        // Funding is a REST series, not a ZIP: its rows are checked through the
        // tape count and its bytes through the checksum.
        let raw_objects: Vec<&ObjectEntry> = manifest.objects.iter().filter(|object| object.kind != "funding").collect();
        let decoded = parallel::map(&raw_objects, jobs, |_, object| decode_object(root, object), |_, _| {})?;
        let deep_ok = decoded.iter().all(Option::is_none);
        let deep_detail = match decoded.iter().flatten().next() {
            Some(detail) => detail.clone(),
            None => "decoded counts match".to_owned(),
        };
        report.record("raw/row-counts", deep_ok, deep_detail);
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

/// Hashes one object and compares it to the manifest. `Some` is the failure.
fn hash_object(root: &Path, object: &ObjectEntry) -> Result<Option<String>> {
    let path = root.join(&object.path);
    if !path.exists() {
        return Ok(Some(format!("{} is missing", object.path)));
    }
    let actual = sha256_file(&path)?;
    if actual != object.sha256 {
        return Ok(Some(format!("{} hashes to {actual}, manifest says {}", object.path, object.sha256)));
    }
    Ok(None)
}

/// Decodes one raw object and compares its row count. `Some` is the failure.
fn decode_object(root: &Path, object: &ObjectEntry) -> Result<Option<String>> {
    let Some(product) = Product::parse(&object.kind) else {
        return Ok(Some(format!("unknown kind {}", object.kind)));
    };
    let stats = decode_with(product, &root.join(&object.path), |_| true)?;
    if stats.rows != object.rows {
        return Ok(Some(format!(
            "{}: decoded {} rows, manifest says {}",
            object.path, stats.rows, object.rows
        )));
    }
    Ok(None)
}

/// Scans one day's tape.
fn scan_day(root: &Path, symbol: &str, day: NaiveDate) -> Result<TapeScan> {
    let path = tape_path(root, symbol, day);
    if !path.exists() {
        return Ok(TapeScan::Missing);
    }
    match scan_tape(&path) {
        Ok((counts, minutes)) => Ok(TapeScan::Scanned(DayTape { counts, minutes })),
        Err(error) => Ok(TapeScan::Failed(error.to_string())),
    }
}

fn parse_iso(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value).ok().map(|moment| moment.timestamp_millis())
}

fn manifest_rows_by_kind(manifest: &Manifest) -> BTreeMap<String, u64> {
    let mut rows: BTreeMap<String, u64> = BTreeMap::new();
    for object in &manifest.objects {
        let kind = Product::parse(&object.kind).map(Product::event_kind).unwrap_or(object.kind.as_str());
        *rows.entry(kind.to_owned()).or_default() += object.rows;
    }
    rows
}

/// Streams one day's tape and checks the per-event invariants.
fn scan_tape(path: &Path) -> Result<(BTreeMap<String, u64>, Vec<i64>)> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let file = File::open(path)?;
    let decoder = zstd::Decoder::new(BufReader::with_capacity(1 << 20, file))?;
    let decoder = BufReader::new(decoder);
    let mut last_ts = i64::MIN;
    let mut last_kind = 0u8;
    // The tape is sorted by (ts, kind, id), not by id: ids are collected and
    // checked for uniqueness after the scan, never for monotonicity.
    let mut ids: BTreeMap<String, Vec<u64>> = BTreeMap::new();
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

    Ok((counts, kline_minutes))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::build;
    use crate::testing::{scratch, write_klines};
    use std::fs;

    /// Two days of klines, built and ready to verify.
    fn dataset(name: &str) -> (std::path::PathBuf, Range) {
        let root = scratch(name);
        let range = Range::parse("2024-01-15", "2024-01-16").unwrap();
        for date in range.iter() {
            write_klines(&root, date, 4);
        }
        build(&root, "BTCUSDT", "1m", range, &[Product::Klines], "test", 2).unwrap();
        (root, range)
    }

    #[test]
    fn a_parallel_verification_reports_the_same_checks() {
        let (root, _) = dataset("verify-parallel");

        let one = verify(&root, true, 1).unwrap();
        let many = verify(&root, true, 4).unwrap();

        assert_eq!(one.checks, many.checks);
        assert!(one.failures.is_empty(), "{:?}", one.failures);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_missing_tape_day_fails_the_structure_check() {
        let (root, range) = dataset("verify-missing-tape");
        fs::remove_file(tape_path(&root, "BTCUSDT", range.from)).unwrap();

        let report = verify(&root, false, 4).unwrap();

        let structure = report
            .checks
            .iter()
            .find(|(name, _, _)| name == "tape/structure")
            .expect("the structure check runs");
        assert!(!structure.1);
        assert!(structure.2.contains(&range.from.to_string()), "detail names the day: {}", structure.2);
        assert!(report.failures.iter().any(|failure| failure.starts_with("tape/structure")));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_corrupt_tape_is_a_failed_check_not_an_aborted_run() {
        let (root, range) = dataset("verify-corrupt-tape");
        let path = tape_path(&root, "BTCUSDT", range.to);
        let mut bytes = fs::read(&path).unwrap();
        bytes.truncate(bytes.len() / 2);
        fs::write(&path, bytes).unwrap();

        let report = verify(&root, false, 2).unwrap();

        assert!(report.failures.iter().any(|failure| failure.starts_with("tape/structure")));
        // The other checks still ran and were reported.
        assert!(report.checks.iter().any(|(name, _, _)| name == "klines/grid"));
        fs::remove_dir_all(root).unwrap();
    }
}
