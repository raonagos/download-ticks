//! Binance Vision archive client.
//!
//! The archive is the only ingress for market data. This module owns the URL
//! layout, the `.CHECKSUM` discipline, atomic writes, idempotency, and
//! quarantine. It never decodes: bytes are stored untouched, exactly as the
//! provider published them.

use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::NaiveDate;
use reqwest::{Client, StatusCode};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;

use crate::errors::{Error, Result};

/// Root of the USD-M archive.
pub const VISION_BASE: &str = "https://data.binance.vision/data/futures/um";

/// One archived product.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Product {
    Klines,
    AggTrades,
    BookTicker,
    BookDepth,
}

impl Product {
    /// The name used in the manifest, the tape and the CLI.
    pub fn as_str(self) -> &'static str {
        match self {
            Product::Klines => "klines",
            Product::AggTrades => "aggTrades",
            Product::BookTicker => "bookTicker",
            Product::BookDepth => "bookDepth",
        }
    }

    /// The tape event kind this product decodes to.
    pub fn event_kind(self) -> &'static str {
        match self {
            Product::Klines => "kline",
            Product::AggTrades => "trade",
            Product::BookTicker => "book_ticker",
            Product::BookDepth => "book_depth",
        }
    }

    /// Parses a `--products` token.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "klines" => Some(Product::Klines),
            "aggTrades" | "aggtrades" => Some(Product::AggTrades),
            "bookTicker" | "bookticker" => Some(Product::BookTicker),
            "bookDepth" | "bookdepth" => Some(Product::BookDepth),
            _ => None,
        }
    }

    /// Directory family under `data/futures/um/daily/`, relative to the symbol.
    fn family(self) -> &'static str {
        match self {
            Product::Klines => "klines",
            Product::AggTrades => "aggTrades",
            Product::BookTicker => "bookTicker",
            Product::BookDepth => "bookDepth",
        }
    }

    /// The provider's file name for one day.
    pub fn file_name(self, symbol: &str, date: NaiveDate, interval: &str) -> String {
        let day = date.format("%Y-%m-%d");
        match self {
            Product::Klines => format!("{symbol}-{interval}-{day}.zip"),
            Product::AggTrades => format!("{symbol}-aggTrades-{day}.zip"),
            Product::BookTicker => format!("{symbol}-bookTicker-{day}.zip"),
            Product::BookDepth => format!("{symbol}-bookDepth-{day}.zip"),
        }
    }

    /// The provider URL for one day.
    pub fn url(self, symbol: &str, date: NaiveDate, interval: &str) -> String {
        let day = date.format("%Y-%m-%d");
        match self {
            Product::Klines => format!("{VISION_BASE}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{day}.zip"),
            Product::AggTrades => format!("{VISION_BASE}/daily/aggTrades/{symbol}/{symbol}-aggTrades-{day}.zip"),
            Product::BookTicker => format!("{VISION_BASE}/daily/bookTicker/{symbol}/{symbol}-bookTicker-{day}.zip"),
            Product::BookDepth => format!("{VISION_BASE}/daily/bookDepth/{symbol}/{symbol}-bookDepth-{day}.zip"),
        }
    }

    /// Where the object lives under the dataset root.
    pub fn raw_path(self, root: &Path, symbol: &str, date: NaiveDate, interval: &str) -> PathBuf {
        let name = self.file_name(symbol, date, interval);
        let dir = root.join("raw/binance/um").join(symbol).join(self.family());
        match self {
            Product::Klines => dir.join(interval).join(name),
            _ => dir.join(name),
        }
    }
}

/// What happened to one object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Bytes were fetched and verified.
    Downloaded,
    /// The object was already present and matched its checksum.
    Skipped,
    /// The provider has no object for that day.
    Missing,
    /// Bytes did not match the checksum; the object is quarantined.
    Quarantined,
}

/// One object's result.
#[derive(Debug, Clone)]
pub struct ObjectResult {
    pub product: Product,
    pub date: NaiveDate,
    pub outcome: Outcome,
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    pub detail: String,
}

impl ObjectResult {
    pub fn is_failure(&self) -> bool {
        matches!(self.outcome, Outcome::Quarantined)
    }
}

/// Syncs one product over a calendar range.
pub struct Archive<'a> {
    client: Client,
    root: &'a Path,
    symbol: &'a str,
    interval: &'a str,
    retries: u32,
}

impl<'a> Archive<'a> {
    pub fn new(root: &'a Path, symbol: &'a str, interval: &'a str, retries: u16) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("download-ticks/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(30 * 60))
            .build()?;
        Ok(Self {
            client,
            root,
            symbol,
            interval,
            retries: u32::from(retries),
        })
    }

    /// Fetches every (product, day) pair, at most `jobs` at a time.
    pub async fn sync(&self, products: &[Product], from: NaiveDate, to: NaiveDate, jobs: usize) -> Result<Vec<ObjectResult>> {
        let jobs = jobs.max(1);
        let mut tasks: JoinSet<Result<ObjectResult>> = JoinSet::new();
        let mut pending = BTreeIter::new(products, from, to);
        let mut results = Vec::new();

        loop {
            while tasks.len() < jobs {
                let Some((product, date)) = pending.next_pair() else {
                    break;
                };
                let client = self.client.clone();
                let root = self.root.to_path_buf();
                let symbol = self.symbol.to_owned();
                let interval = self.interval.to_owned();
                let retries = self.retries;
                tasks.spawn(async move { fetch_object(&client, &root, &symbol, &interval, product, date, retries).await });
            }
            let Some(joined) = tasks.join_next().await else {
                break;
            };
            results.push(joined??);
        }

        results.sort_by_key(|result| (result.date, result.product));
        Ok(results)
    }
}

/// Cartesian product of products and days, yielded in date order.
struct BTreeIter<'a> {
    products: &'a [Product],
    current: NaiveDate,
    to: NaiveDate,
    index: usize,
    done: bool,
}

impl<'a> BTreeIter<'a> {
    fn new(products: &'a [Product], from: NaiveDate, to: NaiveDate) -> Self {
        Self {
            products,
            current: from,
            to,
            index: 0,
            done: false,
        }
    }

    fn next_pair(&mut self) -> Option<(Product, NaiveDate)> {
        if self.done {
            return None;
        }
        let product = self.products[self.index];
        let date = self.current;
        self.index += 1;
        if self.index == self.products.len() {
            self.index = 0;
            match self.current.succ_opt() {
                Some(next) if next <= self.to => self.current = next,
                _ => self.done = true,
            }
        }
        Some((product, date))
    }
}

async fn fetch_object(
    client: &Client,
    root: &Path,
    symbol: &str,
    interval: &str,
    product: Product,
    date: NaiveDate,
    retries: u32,
) -> Result<ObjectResult> {
    let path = product.raw_path(root, symbol, date, interval);
    let url = product.url(symbol, date, interval);

    if path.exists() {
        let stored = sha256_file(&path)?;
        match expected_checksum(client, &url, retries).await? {
            Some(expected) if expected == stored => {
                let bytes = fs::metadata(&path)?.len();
                return Ok(ObjectResult {
                    product,
                    date,
                    outcome: Outcome::Skipped,
                    path,
                    bytes,
                    sha256: stored,
                    detail: "verified".to_owned(),
                });
            }
            Some(expected) => {
                let quarantined = quarantine(&path, &stored)?;
                return Ok(ObjectResult {
                    product,
                    date,
                    outcome: Outcome::Quarantined,
                    path: quarantined,
                    bytes: 0,
                    sha256: stored,
                    detail: format!("bytes do not match {expected}; quarantined"),
                });
            }
            None => {
                // No published checksum: keep the bytes, but say so.
                let bytes = fs::metadata(&path)?.len();
                return Ok(ObjectResult {
                    product,
                    date,
                    outcome: Outcome::Skipped,
                    path,
                    bytes,
                    sha256: stored,
                    detail: "no published checksum".to_owned(),
                });
            }
        }
    }

    let Some(expected) = expected_checksum(client, &url, retries).await? else {
        // The provider has no checksum for this day: the object does not exist.
        let status = head_status(client, &url, retries).await?;
        return Ok(ObjectResult {
            product,
            date,
            outcome: Outcome::Missing,
            path,
            bytes: 0,
            sha256: String::new(),
            detail: format!(
                "no archive object (HTTP {})",
                status.map_or("none".to_owned(), |s| s.as_u16().to_string())
            ),
        });
    };

    let bytes = download_to_part(client, &url, &path, retries).await?;
    let stored = sha256_file(&path)?;
    if stored != expected {
        let quarantined = quarantine(&path, &stored)?;
        return Ok(ObjectResult {
            product,
            date,
            outcome: Outcome::Quarantined,
            path: quarantined,
            bytes,
            sha256: stored,
            detail: format!("downloaded bytes do not match {expected}; quarantined"),
        });
    }
    fs::write(
        path.with_extension("zip.sha256"),
        format!("{stored}  {}\n", path.file_name().unwrap_or_default().to_string_lossy()),
    )?;

    Ok(ObjectResult {
        product,
        date,
        outcome: Outcome::Downloaded,
        path,
        bytes,
        sha256: stored,
        detail: "downloaded and verified".to_owned(),
    })
}

/// Reads the provider's `<url>.CHECKSUM`, which is `<sha256>  <file>`.
async fn expected_checksum(client: &Client, url: &str, retries: u32) -> Result<Option<String>> {
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
                if attempt >= retries {
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
                if attempt >= retries {
                    return Err(Error::Reqwest(error));
                }
            }
        }
        attempt += 1;
        tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
    }
}

async fn head_status(client: &Client, url: &str, retries: u32) -> Result<Option<StatusCode>> {
    let mut attempt = 0;
    loop {
        match client.head(url).send().await {
            Ok(response) => return Ok(Some(response.status())),
            Err(error) => {
                if attempt >= retries {
                    return Err(Error::Reqwest(error));
                }
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
        }
    }
}

/// Streams one object to `<path>.part`, then renames it into place.
async fn download_to_part(client: &Client, url: &str, path: &Path, retries: u32) -> Result<u64> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let part = path.with_extension("zip.part");
    let mut attempt = 0;
    loop {
        match attempt_download(client, url, &part).await {
            Ok(bytes) => {
                fs::rename(&part, path)?;
                return Ok(bytes);
            }
            Err(error) => {
                let _ = fs::remove_file(&part);
                if attempt >= retries {
                    return Err(error);
                }
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(1000 * u64::from(attempt))).await;
            }
        }
    }
}

async fn attempt_download(client: &Client, url: &str, part: &Path) -> Result<u64> {
    let mut response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(Error::HttpStatus {
            status: response.status(),
            url: url.to_owned(),
        });
    }
    let mut file = File::create(part)?;
    let mut bytes = 0u64;
    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk)?;
        bytes += chunk.len() as u64;
    }
    file.sync_all()?;
    Ok(bytes)
}

/// Moves a corrupt object aside with both hashes in its name.
fn quarantine(path: &Path, actual: &str) -> Result<PathBuf> {
    let target = path.with_extension(format!("zip.quarantine-{}", &actual[..16.min(actual.len())]));
    fs::rename(path, &target)?;
    Ok(target)
}

/// Hex SHA-256 of a file.
pub fn sha256_file(path: &Path) -> Result<String> {
    let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// A calendar range, inclusive.
#[derive(Debug, Clone, Copy)]
pub struct Range {
    pub from: NaiveDate,
    pub to: NaiveDate,
}

impl Range {
    pub fn parse(from: &str, to: &str) -> Result<Self> {
        let from = parse_day(from)?;
        let to = parse_day(to)?;
        if to < from {
            return Err(Error::InvalidDatetime);
        }
        Ok(Self { from, to })
    }

    pub fn iter(&self) -> impl Iterator<Item = NaiveDate> {
        let from = self.from;
        let to = self.to;
        std::iter::successors(Some(from), move |day| {
            let next = day.succ_opt()?;
            (next <= to).then_some(next)
        })
    }
}

fn parse_day(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .or_else(|_| chrono::DateTime::parse_from_rfc3339(value).map(|moment| moment.date_naive()))
        .map_err(|_| Error::InvalidDatetime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_match_the_documented_layout() {
        let day = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(
            Product::Klines.url("BTCUSDT", day, "1m"),
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-15.zip"
        );
        assert_eq!(
            Product::BookDepth.url("BTCUSDT", day, "1m"),
            "https://data.binance.vision/data/futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2024-01-15.zip"
        );
    }

    #[test]
    fn raw_paths_follow_the_contract() {
        let day = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let root = Path::new("data");
        assert_eq!(
            Product::Klines.raw_path(root, "BTCUSDT", day, "1m"),
            Path::new("data/raw/binance/um/BTCUSDT/klines/1m/BTCUSDT-1m-2024-01-15.zip")
        );
        assert_eq!(
            Product::AggTrades.raw_path(root, "BTCUSDT", day, "1m"),
            Path::new("data/raw/binance/um/BTCUSDT/aggTrades/BTCUSDT-aggTrades-2024-01-15.zip")
        );
    }

    #[test]
    fn range_iterates_inclusively() {
        let range = Range::parse("2024-01-01", "2024-01-03").unwrap();
        let days: Vec<_> = range.iter().collect();
        assert_eq!(days.len(), 3);
    }

    #[test]
    fn range_rejects_an_inverted_bounds() {
        assert!(Range::parse("2024-02-01", "2024-01-01").is_err());
    }

    #[test]
    fn pair_iteration_covers_product_times_day() {
        let from = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let products = [Product::Klines, Product::BookTicker];
        let mut iter = BTreeIter::new(&products, from, to);
        let mut seen = Vec::new();
        while let Some(pair) = iter.next_pair() {
            seen.push(pair);
        }
        assert_eq!(seen.len(), 4);
        assert!(seen.contains(&(Product::Klines, from)));
        assert!(seen.contains(&(Product::BookTicker, to)));
    }
}
