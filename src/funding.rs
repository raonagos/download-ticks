//! Funding stamps from the REST endpoint.
//!
//! The archive publishes no funding series, so this is the one place the tool
//! talks to a live API. The output is normalised: `ts_ms,rate`, unique and
//! ordered, written atomically. `rate` is a fraction, not a percentage.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

use crate::errors::{Error, Result};

const FUNDING_URL: &str = "https://fapi.binance.com/fapi/v1/fundingRate";
const PAGE_LIMIT: usize = 1000;

#[derive(Debug, Deserialize)]
struct FundingRow {
    #[serde(rename = "fundingTime")]
    funding_time: i64,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
}

/// Where the normalised series lives.
pub fn funding_path(root: &Path, symbol: &str) -> PathBuf {
    root.join("funding/binance/um").join(format!("{symbol}.csv"))
}

/// Fetches every stamp in `[from_ms, to_ms)` and writes the CSV.
pub async fn fetch(root: &Path, symbol: &str, from_ms: i64, to_ms: i64) -> Result<usize> {
    let client = Client::builder()
        .user_agent(concat!("download-ticks/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut stamps: Vec<(i64, f64)> = Vec::new();
    let mut cursor = from_ms;
    loop {
        let url = format!("{FUNDING_URL}?symbol={symbol}&startTime={cursor}&limit={PAGE_LIMIT}");
        let response = client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(Error::HttpStatus {
                status: response.status(),
                url,
            });
        }
        let rows: Vec<FundingRow> = response.json().await?;
        if rows.is_empty() {
            break;
        }
        let mut advanced = false;
        for row in &rows {
            if row.funding_time < from_ms || row.funding_time >= to_ms {
                continue;
            }
            let rate = row.funding_rate.parse::<f64>().map_err(|_| Error::MalformedRow)?;
            if rate.is_finite() {
                stamps.push((row.funding_time, rate));
                advanced = true;
            }
        }
        let last = rows.last().map(|row| row.funding_time).unwrap_or(cursor);
        if last < cursor {
            break;
        }
        cursor = last + 1;
        if !advanced && rows.len() < PAGE_LIMIT {
            break;
        }
        if cursor >= to_ms {
            break;
        }
    }

    stamps.sort_by_key(|(ts, _)| *ts);
    stamps.dedup_by_key(|(ts, _)| *ts);

    let path = funding_path(root, symbol);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let part = path.with_extension("csv.part");
    let mut file = fs::File::create(&part)?;
    writeln!(file, "ts_ms,rate")?;
    for (ts, rate) in &stamps {
        writeln!(file, "{ts},{rate}")?;
    }
    file.sync_all()?;
    drop(file);
    fs::rename(&part, &path)?;
    Ok(stamps.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn funding_path_follows_the_contract() {
        assert_eq!(
            funding_path(Path::new("data"), "BTCUSDT"),
            Path::new("data/funding/binance/um/BTCUSDT.csv")
        );
    }
}
