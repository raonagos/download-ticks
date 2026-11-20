//! Funding stamps.
//!
//! The canonical source is the Vision archive: one monthly ZIP per symbol,
//! published with a `.CHECKSUM` sibling and normalised as
//! `calc_time,funding_interval_hours,last_funding_rate`. It is checksummed,
//! reproducible and reachable from everywhere.
//!
//! The REST endpoint is kept as an explicit fallback (`--source rest`) because
//! some networks answer it with `451 Unavailable For Legal Reasons`, and a
//! fallback that silently returns nothing would be worse than a loud failure.
//!
//! The output is normalised: `ts_ms,rate`, unique and ordered, written
//! atomically. `rate` is a fraction, not a percentage.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{Datelike, TimeZone, Utc};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::archive::VISION_BASE;
use crate::errors::{Error, Result};

/// Where the stamps come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Source {
    /// Vision monthly archive (default): checksummed, reproducible.
    Archive,
    /// Live REST endpoint; may be geo-blocked.
    Rest,
}

/// REST hosts tried in order. `fapi.binance.com` is canonical; the second host
/// serves the same path and is not blocked in every region.
const REST_BASES: [&str; 2] = ["https://fapi.binance.com", "https://www.binance.com"];
const REST_PATH: &str = "/fapi/v1/fundingRate";
const PAGE_LIMIT: usize = 1000;
const RETRIES: u32 = 3;

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

/// Where one archived month lives under the dataset root.
pub fn archive_path(root: &Path, symbol: &str, year: i32, month: u32) -> PathBuf {
    let name = format!("{symbol}-fundingRate-{year:04}-{month:02}.zip");
    root.join("raw/binance/um").join(symbol).join("fundingRate").join(name)
}

/// The provider URL for one archived month.
pub fn archive_url(symbol: &str, year: i32, month: u32) -> String {
    format!(
        "{VISION_BASE}/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{year:04}-{month:02}.zip"
    )
}

/// Fetches every stamp in `[from_ms, to_ms)` and writes the CSV.
pub async fn fetch(root: &Path, symbol: &str, from_ms: i64, to_ms: i64, source: Source) -> Result<usize> {
    let stamps = match source {
        Source::Archive => archive_stamps(root, symbol, from_ms, to_ms).await?,
        Source::Rest => rest_stamps(symbol, from_ms, to_ms).await?,
    };

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

/// Every `(year, month)` touched by `[from_ms, to_ms)`.
fn months(from_ms: i64, to_ms: i64) -> Result<Vec<(i32, u32)>> {
    let from = Utc
        .timestamp_millis_opt(from_ms)
        .single()
        .ok_or(Error::InvalidDatetime)?;
    let mut year = from.year();
    let mut month = from.month();
    let mut out = Vec::new();
    loop {
        let start = Utc
            .with_ymd_and_hms(year, month, 1, 0, 0, 0)
            .single()
            .ok_or(Error::InvalidDatetime)?
            .timestamp_millis();
        if start >= to_ms {
            break;
        }
        out.push((year, month));
        if month == 12 {
            year += 1;
            month = 1;
        } else {
            month += 1;
        }
    }
    Ok(out)
}

/// Reads `calc_time,funding_interval_hours,last_funding_rate` rows into stamps.
fn parse_archive_csv(text: &str, from_ms: i64, to_ms: i64, out: &mut BTreeMap<i64, f64>) -> Result<()> {
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("calc_time") {
            continue;
        }
        let mut fields = line.split(',');
        let ts = fields
            .next()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .ok_or(Error::MalformedRow)?;
        let _interval = fields.next();
        let rate = fields
            .next()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .ok_or(Error::MalformedRow)?;
        if !rate.is_finite() || ts < from_ms || ts >= to_ms {
            continue;
        }
        out.insert(ts, rate);
    }
    Ok(())
}

async fn archive_stamps(root: &Path, symbol: &str, from_ms: i64, to_ms: i64) -> Result<BTreeMap<i64, f64>> {
    let client = client()?;
    let mut stamps = BTreeMap::new();
    for (year, month) in months(from_ms, to_ms)? {
        let url = archive_url(symbol, year, month);
        let path = archive_path(root, symbol, year, month);
        match ensure_object(&client, &url, &path).await? {
            // The provider has not published this month (yet, or ever).
            false => continue,
            true => {}
        }
        let text = read_zip_entry(&path)?;
        parse_archive_csv(&text, from_ms, to_ms, &mut stamps)?;
    }
    Ok(stamps)
}

/// Makes sure one archive object is on disk and matches its published checksum.
/// Returns `false` when the provider has no such object.
async fn ensure_object(client: &Client, url: &str, path: &Path) -> Result<bool> {
    let Some(expected) = expected_checksum(client, url).await? else {
        return Ok(false);
    };
    if path.exists() {
        let stored = sha256_file(path)?;
        if stored == expected {
            return Ok(true);
        }
        // A wrong local object is not trusted: drop it and fetch it again.
        fs::remove_file(path)?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let part = path.with_extension("zip.part");
    let (_, stored) = download(client, url, &part).await?;
    if stored != expected {
        let _ = fs::remove_file(&part);
        return Err(Error::Manifest(format!(
            "funding archive {url}: bytes do not match {expected}; not stored"
        )));
    }
    fs::rename(&part, path)?;
    fs::write(
        path.with_extension("zip.sha256"),
        format!("{stored}  {}\n", path.file_name().unwrap_or_default().to_string_lossy()),
    )?;
    Ok(true)
}

async fn download(client: &Client, url: &str, part: &Path) -> Result<(u64, String)> {
    let mut attempt = 0;
    loop {
        match attempt_download(client, url, part).await {
            Ok(done) => return Ok(done),
            Err(error) => {
                let _ = fs::remove_file(part);
                if attempt >= RETRIES {
                    return Err(error);
                }
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(1000 * u64::from(attempt))).await;
            }
        }
    }
}

async fn attempt_download(client: &Client, url: &str, part: &Path) -> Result<(u64, String)> {
    let mut response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(Error::HttpStatus {
            status: response.status(),
            url: url.to_owned(),
        });
    }
    let mut file = fs::File::create(part)?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk)?;
        hasher.update(&chunk);
        bytes += chunk.len() as u64;
    }
    file.sync_all()?;
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

/// Reads the provider's `<url>.CHECKSUM`; `None` when the object does not exist.
async fn expected_checksum(client: &Client, url: &str) -> Result<Option<String>> {
    let checksum_url = format!("{url}.CHECKSUM");
    let mut attempt = 0;
    loop {
        match client.get(&checksum_url).send().await {
            Ok(response) if response.status().is_success() => {
                let text = response.text().await?;
                let digest = text.split_whitespace().next().unwrap_or_default().to_ascii_lowercase();
                return Ok(Some(digest).filter(|digest| digest.len() == 64));
            }
            Ok(response) if response.status() == StatusCode::NOT_FOUND => return Ok(None),
            Ok(response) if response.status().is_server_error() || response.status() == StatusCode::TOO_MANY_REQUESTS => {
                if attempt >= RETRIES {
                    return Err(Error::HttpStatus {
                        status: response.status(),
                        url: checksum_url,
                    });
                }
            }
            Ok(response) => {
                return Err(Error::HttpStatus {
                    status: response.status(),
                    url: checksum_url,
                });
            }
            Err(error) => {
                if attempt >= RETRIES {
                    return Err(Error::Reqwest(error));
                }
            }
        }
        attempt += 1;
        tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
    }
}

fn read_zip_entry(path: &Path) -> Result<String> {
    let file = fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(std::io::BufReader::new(file)).map_err(Error::Zip)?;
    if archive.is_empty() {
        return Err(Error::Manifest(format!("funding archive {} is empty", path.display())));
    }
    let mut entry = archive.by_index(0).map_err(Error::Zip)?;
    let mut text = String::new();
    entry.read_to_string(&mut text)?;
    Ok(text)
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

async fn rest_stamps(symbol: &str, from_ms: i64, to_ms: i64) -> Result<BTreeMap<i64, f64>> {
    let client = client()?;
    let mut stamps = BTreeMap::new();
    let mut base = None;
    let mut cursor = from_ms;
    loop {
        let mut page = None;
        for candidate in REST_BASES {
            if base.is_some_and(|chosen| chosen != candidate) {
                continue;
            }
            let url = format!("{candidate}{REST_PATH}?symbol={symbol}&startTime={cursor}&limit={PAGE_LIMIT}");
            let response = client.get(&url).send().await?;
            match response.status() {
                status if status.is_success() => {
                    base = Some(candidate);
                    page = Some((url, response.json::<Vec<FundingRow>>().await?));
                    break;
                }
                // Geo-blocked or forbidden host: try the other one.
                StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS | StatusCode::FORBIDDEN => continue,
                status => {
                    return Err(Error::HttpStatus { status, url });
                }
            }
        }
        let Some((_, rows)) = page else {
            return Err(Error::Manifest(
                "funding REST endpoint is unreachable: pass --source archive".to_owned(),
            ));
        };
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
                stamps.insert(row.funding_time, rate);
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
    Ok(stamps)
}

fn client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent(concat!("download-ticks/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(60))
        .build()?)
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

    #[test]
    fn archive_url_uses_the_monthly_family() {
        assert_eq!(
            archive_url("BTCUSDT", 2024, 1),
            "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip"
        );
    }

    #[test]
    fn archive_path_is_under_the_raw_root() {
        assert_eq!(
            archive_path(Path::new("data"), "BTCUSDT", 2024, 1),
            Path::new("data/raw/binance/um/BTCUSDT/fundingRate/BTCUSDT-fundingRate-2024-01.zip")
        );
    }

    #[test]
    fn months_cover_a_span_across_a_year() {
        let from = Utc.with_ymd_and_hms(2023, 11, 20, 0, 0, 0).unwrap().timestamp_millis();
        let to = Utc.with_ymd_and_hms(2024, 2, 3, 0, 0, 0).unwrap().timestamp_millis();
        assert_eq!(months(from, to).unwrap(), vec![(2023, 11), (2023, 12), (2024, 1), (2024, 2)]);
    }

    #[test]
    fn the_archive_csv_is_parsed_and_filtered() {
        let text = "calc_time,funding_interval_hours,last_funding_rate\n\
                    1704067200000,8,0.00037409\n\
                    1704096000000,8,0.00027213\n\
                    1704124800000,8,0.00033601\n";
        let mut stamps = BTreeMap::new();
        parse_archive_csv(text, 1704067200000, 1704124800000, &mut stamps).unwrap();
        assert_eq!(stamps.len(), 2);
        assert_eq!(stamps[&1704067200000], 0.00037409);
        assert!(!stamps.contains_key(&1704124800000));
    }

    #[test]
    fn a_malformed_row_is_a_loud_error() {
        let mut stamps = BTreeMap::new();
        assert!(parse_archive_csv("calc_time,funding_interval_hours,last_funding_rate\nnope,8,0.1\n", 0, i64::MAX, &mut stamps).is_err());
    }
}
