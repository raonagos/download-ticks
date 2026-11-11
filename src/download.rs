//! Bounded, resumable-friendly downloading for exchange endpoints.
//!
//! Every URL is fetched by its own Tokio task. A sliding window of at most
//! `jobs` tasks is kept in flight, so concurrency is bounded by construction
//! without a shared semaphore. Results are written to the output in request
//! order, even though they complete out of order, and the output is only
//! renamed into place once every chunk has been written.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use indicatif::ProgressBar;
use reqwest::header::RETRY_AFTER;
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;

use crate::errors::{Error, Result};
use crate::market::Kline;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const BASE_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// A summary of a completed download.
#[derive(Debug, Default)]
pub struct DownloadReport {
    pub requests: usize,
    pub rows: usize,
    pub first_open: Option<DateTime<Utc>>,
    pub last_close: Option<DateTime<Utc>>,
    pub elapsed: Duration,
}

/// Downloads JSON chunks with bounded concurrency and retries.
pub struct Downloader {
    client: Client,
    retries: u32,
}

impl Downloader {
    /// Builds a downloader with a shared connection pool.
    ///
    /// `retries` is the number of retries after the first attempt.
    pub fn new(retries: u16) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("download-ticks/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .pool_max_idle_per_host(32)
            .build()?;

        Ok(Self {
            client,
            retries: u32::from(retries),
        })
    }

    /// Fetches every URL and writes the chunks to `output` as one JSON array.
    ///
    /// At most `jobs` requests are in flight. Chunks are buffered only while an
    /// earlier chunk is still missing, so memory stays bounded even when the
    /// response is larger than the window. If `output` is `None`, the data is
    /// still fetched and counted but not persisted.
    pub async fn download<T>(&self, urls: Vec<String>, jobs: usize, output: Option<&Path>, progress: Option<&ProgressBar>) -> Result<DownloadReport>
    where
        T: DeserializeOwned + Serialize + Kline + Send + 'static,
    {
        let started = Instant::now();
        let jobs = jobs.max(1);
        let mut writer = output.map(JsonArrayWriter::create).transpose()?;
        let mut tasks: JoinSet<(usize, Result<Vec<T>>)> = JoinSet::new();
        let mut pending = urls.into_iter().enumerate();
        let mut buffered: BTreeMap<usize, Vec<T>> = BTreeMap::new();
        let mut next_chunk = 0usize;
        let mut report = DownloadReport::default();

        loop {
            // Keep the window full. Spawning more work than `jobs` is the only
            // thing that would break the bound.
            while tasks.len() < jobs {
                let Some((index, url)) = pending.next() else {
                    break;
                };
                let client = self.client.clone();
                let retries = self.retries;
                tasks.spawn(async move {
                    let result = fetch_json::<T>(&client, &url, retries).await;
                    (index, result)
                });
            }

            let Some(joined) = tasks.join_next().await else {
                break;
            };
            let (index, result) = joined?;
            let chunk = result?;

            report.requests += 1;
            if let Some(progress) = progress {
                progress.inc(1);
            }

            buffered.insert(index, chunk);
            // Write every chunk that is now contiguous with what was written.
            while let Some(chunk) = buffered.remove(&next_chunk) {
                if let Some(writer) = writer.as_mut() {
                    writer.write_chunk(&chunk)?;
                }
                report.rows += chunk.len();
                if let Some(first) = chunk.first() {
                    report.first_open.get_or_insert_with(|| first.open_time());
                }
                if let Some(last) = chunk.last() {
                    report.last_close = Some(last.close_time());
                }
                next_chunk += 1;
            }
        }

        if let Some(writer) = writer {
            writer.finish()?;
        }

        report.elapsed = started.elapsed();
        Ok(report)
    }
}

/// Fetches one JSON array, retrying transient failures with backoff.
async fn fetch_json<T>(client: &Client, url: &str, retries: u32) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let mut attempt = 0u32;
    loop {
        match attempt_json(client, url).await {
            Ok(data) => return Ok(data),
            Err(AttemptError::Status { status, retry_after }) => {
                if attempt >= retries || !is_retryable(status) {
                    return Err(Error::HttpStatus { status, url: url.to_owned() });
                }
                tokio::time::sleep(retry_delay(attempt, retry_after)).await;
            }
            Err(AttemptError::Transport(error)) => {
                if attempt >= retries {
                    return Err(Error::Reqwest(error));
                }
                tokio::time::sleep(backoff(attempt)).await;
            }
        }
        attempt += 1;
    }
}

async fn attempt_json<T>(client: &Client, url: &str) -> std::result::Result<Vec<T>, AttemptError>
where
    T: DeserializeOwned,
{
    let response = client.get(url).send().await.map_err(AttemptError::Transport)?;
    let status = response.status();

    if !status.is_success() {
        return Err(AttemptError::Status {
            status,
            retry_after: retry_after(&response),
        });
    }

    response.json().await.map_err(AttemptError::Transport)
}

enum AttemptError {
    Status { status: StatusCode, retry_after: Option<Duration> },
    Transport(reqwest::Error),
}

/// A status is transient when retrying it can plausibly succeed.
fn is_retryable(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT || status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let seconds = response.headers().get(RETRY_AFTER)?.to_str().ok()?.trim().parse::<u64>().ok()?;

    Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}

fn retry_delay(attempt: u32, retry_after: Option<Duration>) -> Duration {
    retry_after.unwrap_or_else(|| backoff(attempt))
}

/// Exponential backoff with a little jitter, capped at [`MAX_BACKOFF`].
fn backoff(attempt: u32) -> Duration {
    let factor = 1u32 << attempt.min(6);
    let base = BASE_BACKOFF.saturating_mul(factor);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_millis())
        .unwrap_or(0);

    (base + Duration::from_millis(u64::from(jitter))).min(MAX_BACKOFF)
}

/// Writes one JSON array incrementally and renames it into place on success.
///
/// The caller owns chunk ordering; this type only serialises items. If the
/// writer is dropped before [`finish`](Self::finish), the `.part` file is
/// removed, so a failed run never leaves a half-written output visible.
pub struct JsonArrayWriter {
    writer: Option<BufWriter<File>>,
    part_path: PathBuf,
    final_path: PathBuf,
    started: bool,
    finished: bool,
}

impl JsonArrayWriter {
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }

        let file_name = path.file_name().ok_or_else(|| Error::InvalidOutputPath(path.to_path_buf()))?;
        let mut part_name = file_name.to_os_string();
        part_name.push(".part");
        let part_path = path.with_file_name(part_name);

        let file = File::create(&part_path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(b"[")?;

        Ok(Self {
            writer: Some(writer),
            part_path,
            final_path: path.to_path_buf(),
            started: false,
            finished: false,
        })
    }

    /// Appends items to the array, inserting separators as needed.
    pub fn write_chunk<T: Serialize>(&mut self, items: &[T]) -> Result<()> {
        let writer = self.writer.as_mut().expect("JsonArrayWriter used after finish");
        for item in items {
            if self.started {
                writer.write_all(b",")?;
            }
            serde_json::to_writer(&mut *writer, item)?;
            self.started = true;
        }
        Ok(())
    }

    /// Closes the array, flushes it, and atomically moves it to its final path.
    pub fn finish(mut self) -> Result<()> {
        let mut writer = self.writer.take().expect("JsonArrayWriter finished twice");
        writer.write_all(b"]")?;
        writer.flush()?;
        let file = writer.into_inner().map_err(|error| error.into_error())?;
        file.sync_all()?;
        drop(file);

        fs::rename(&self.part_path, &self.final_path)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for JsonArrayWriter {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Close the file before unlinking it.
        self.writer.take();
        let _ = fs::remove_file(&self.part_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("download-ticks-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn backoff_grows_and_stays_capped() {
        assert!(backoff(0) < backoff(3));
        assert!(backoff(10) <= MAX_BACKOFF);
    }

    #[test]
    fn retries_only_transient_statuses() {
        assert!(is_retryable(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable(StatusCode::REQUEST_TIMEOUT));
        assert!(!is_retryable(StatusCode::NOT_FOUND));
        assert!(!is_retryable(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn writer_produces_a_valid_array() {
        let dir = temp_dir("writer");
        let path = dir.join("out.json");

        let mut writer = JsonArrayWriter::create(&path).unwrap();
        writer.write_chunk(&[Value::from(1), Value::from(2)]).unwrap();
        writer.write_chunk::<Value>(&[]).unwrap();
        writer.write_chunk(&[Value::from(3)]).unwrap();
        writer.finish().unwrap();

        let data: Vec<Value> = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(data, vec![Value::from(1), Value::from(2), Value::from(3)]);
        assert!(!dir.join("out.json.part").exists());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn dropping_the_writer_removes_the_partial_file() {
        let dir = temp_dir("drop");
        let path = dir.join("out.json");
        let writer = JsonArrayWriter::create(&path).unwrap();
        drop(writer);

        assert!(!dir.join("out.json.part").exists());
        assert!(!path.exists());

        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn fetch_json_retries_a_server_error() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let response = if attempt == 0 {
                    "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n[]"
                };
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });

        let client = Client::builder().no_proxy().build().unwrap();
        let url = format!("http://{address}/klines");
        let data: Vec<Value> = fetch_json(&client, &url, 1).await.unwrap();

        assert!(data.is_empty());
        server.await.unwrap();
    }
}
