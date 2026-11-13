//! The dataset manifest: identity, provenance, and the gap list.
//!
//! `dataset_id` is the SHA-256 of the canonical document with the identity
//! fields blanked. `created_utc` is blanked too: it records when the build ran,
//! not what the data is, and data-contract §9 requires the hash to survive a
//! rebuild.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::errors::{Error, Result};

pub const SCHEMA: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub schema: u32,
    pub dataset_id: String,
    pub created_utc: String,
    pub tool: Tool,
    pub venue: String,
    pub market: String,
    pub symbol: String,
    pub range: RangeJson,
    pub objects: Vec<ObjectEntry>,
    pub gaps: Vec<Gap>,
    pub totals: Totals,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub version: String,
    pub commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeJson {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    pub path: String,
    pub sha256: String,
    pub rows: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub bytes: u64,
    pub source: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gap {
    pub kind: String,
    pub from: String,
    pub to: String,
    pub missing: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Totals {
    pub objects: usize,
    pub rows: u64,
    pub bytes: u64,
    pub gaps: usize,
    pub duplicates: u64,
}

impl Manifest {
    /// Recomputes `dataset_id` from the document itself.
    pub fn recompute_id(&mut self) -> Result<String> {
        let mut canonical = self.clone();
        canonical.dataset_id = String::new();
        canonical.created_utc = String::new();
        let bytes = serde_json::to_vec(&canonical)?;
        let digest = Sha256::digest(&bytes);
        Ok(format!("sha256:{digest:x}"))
    }

    /// Writes the manifest atomically (`.part` then rename).
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let part = path.with_extension("json.part");
        let mut file = File::create(&part)?;
        serde_json::to_writer_pretty(&mut file, self)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&part, path)?;
        Ok(())
    }

    pub fn read(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)?;
        serde_json::from_str(&text).map_err(|error| Error::Manifest(error.to_string()))
    }

    pub fn path(root: &Path) -> PathBuf {
        root.join("manifest.json")
    }
}

/// Missing 1-minute klines of one UTC day, as `(open_ts_ms, missing)` runs.
///
/// `present` is the sorted list of `open_time` values that closed that day; the
/// expectation is the 1,440 minutes from 00:00 to 23:59. A gap is *reported*,
/// never filled: the tape simply has no kline event there.
pub fn kline_gaps(day_start_ms: i64, present: &[i64]) -> Vec<(i64, i64)> {
    const MINUTE: i64 = 60_000;
    let mut gaps = Vec::new();
    let mut cursor = day_start_ms;
    let expected_end = day_start_ms + 1_440 * MINUTE;
    for &open in present {
        if open < cursor {
            continue;
        }
        if open > cursor {
            let missing = (open - cursor) / MINUTE;
            if missing > 0 {
                gaps.push((cursor, missing));
            }
        }
        cursor = open + MINUTE;
    }
    if cursor < expected_end {
        gaps.push((cursor, (expected_end - cursor) / MINUTE));
    }
    gaps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_day_has_no_gap() {
        let start = 1_705_276_800_000i64;
        let present: Vec<i64> = (0..1440).map(|minute| start + minute * 60_000).collect();
        assert!(kline_gaps(start, &present).is_empty());
    }

    #[test]
    fn a_missing_hour_is_one_gap_of_sixty() {
        let start = 1_705_276_800_000i64;
        let present: Vec<i64> = (0..1440)
            .filter(|minute| !(120..180).contains(minute))
            .map(|minute| start + minute * 60_000)
            .collect();
        let gaps = kline_gaps(start, &present);
        assert_eq!(gaps, vec![(start + 120 * 60_000, 60)]);
    }

    #[test]
    fn a_late_start_is_a_gap_at_the_head() {
        let start = 1_705_276_800_000i64;
        let present: Vec<i64> = (10..1440).map(|minute| start + minute * 60_000).collect();
        assert_eq!(kline_gaps(start, &present), vec![(start, 10)]);
    }
}
