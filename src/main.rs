//! A command-line tool to fetch and save candlestick (kline) data from exchanges.
//!
//! The downloader runs bounded concurrent requests on the Tokio runtime, streams
//! completed chunks to a JSON array in request order, retries transient errors,
//! and atomically publishes the output file.
//!
//! ## Usage
//! The tool is designed to be flexible and easy to use. See the `cli` module for
//! command-line options.

mod archive;
mod build;
mod cli;
mod decode;
mod download;
mod errors;
mod funding;
mod manifest;
mod market;
mod tape;
mod utils;
mod verify;

use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::archive::{Archive, Product, Range};
use crate::cli::*;
use crate::download::Downloader;
use crate::errors::*;
use crate::market::*;
use crate::utils::*;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::build()?;
    match &cli.command {
        Commands::Info(command) => info(command)?,
        Commands::Fetch(command) => fetch(command).await?,
        Commands::Sync(command) => sync_archive(command).await?,
        Commands::Build(command) => build_dataset(command)?,
        Commands::Verify(command) => verify_dataset(command)?,
        Commands::Funding(command) => fetch_funding(command).await?,
    }

    Ok(())
}

/// Parses the `--products` list.
fn parse_products(value: &str) -> Result<Vec<Product>> {
    let mut products = Vec::new();
    for token in value.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let product = Product::parse(token).ok_or_else(|| Error::Manifest(format!("unknown product {token}")))?;
        if !products.contains(&product) {
            products.push(product);
        }
    }
    if products.is_empty() {
        return Err(Error::Manifest("no product selected".to_owned()));
    }
    Ok(products)
}

/// Downloads the raw archive objects and reports every outcome.
async fn sync_archive(command: &SyncCommand) -> Result<()> {
    let range = Range::parse(&command.from_date, &command.to_date)?;
    let products = parse_products(&command.products)?;
    let archive = Archive::new(&command.root, &command.symbol, &command.interval, command.retries)?;
    let started = std::time::Instant::now();
    let results = archive.sync(&products, range.from, range.to, usize::from(command.jobs)).await?;

    let mut downloaded = 0usize;
    let mut skipped = 0usize;
    let mut missing = 0usize;
    let mut failed = 0usize;
    let mut bytes = 0u64;
    for result in &results {
        match result.outcome {
            crate::archive::Outcome::Downloaded => downloaded += 1,
            crate::archive::Outcome::Skipped => skipped += 1,
            crate::archive::Outcome::Missing => missing += 1,
            crate::archive::Outcome::Quarantined => failed += 1,
        }
        bytes += result.bytes;
        if command.verbose || result.is_failure() {
            println!(
                "{} {} {:?} {} {} ({})",
                result.date,
                result.product.as_str(),
                result.outcome,
                build::human_bytes(result.bytes),
                result.detail,
                result.path.display()
            );
            if result.is_failure() {
                println!("    actual sha256: {}", result.sha256);
            }
        }
    }

    println!(
        "{} objects: {} downloaded, {} already present, {} missing, {} failed; {} in {}.",
        results.len(),
        downloaded,
        skipped,
        missing,
        failed,
        build::human_bytes(bytes),
        format_elapsed(started.elapsed())
    );

    if failed > 0 {
        return Err(Error::Manifest(format!(
            "{failed} object(s) failed their checksum; see the quarantined files"
        )));
    }
    Ok(())
}

/// Builds the tape and the manifest from the raw objects.
fn build_dataset(command: &BuildCommand) -> Result<()> {
    let range = Range::parse(&command.from_date, &command.to_date)?;
    let products = parse_products(&command.products)?;
    let commit = tool_commit();
    let report = build::build(&command.root, &command.symbol, &command.interval, range, &products, &commit)?;
    let manifest = &report.manifest;

    println!(
        "
{} days, {} events, tape {} in {}.",
        report.days,
        separator(report.events, ",")?,
        build::human_bytes(report.tape_bytes),
        format_elapsed(std::time::Duration::from_millis(report.elapsed_ms as u64))
    );
    println!(
        "manifest: {} objects, {} rows, {} raw, {} gaps",
        manifest.totals.objects,
        separator(manifest.totals.rows, ",")?,
        build::human_bytes(manifest.totals.bytes),
        manifest.totals.gaps
    );
    println!("dataset_id: {}", manifest.dataset_id);
    Ok(())
}

/// Runs the contract's verification and exits non-zero on a failure.
fn verify_dataset(command: &VerifyCommand) -> Result<()> {
    let report = verify::verify(&command.root, command.deep)?;
    for (name, ok, detail) in &report.checks {
        println!("{} {name}: {detail}", if *ok { "ok  " } else { "FAIL" });
    }
    if report.failures.is_empty() {
        println!(
            "
dataset verified."
        );
        Ok(())
    } else {
        Err(Error::Verify(format!("{} check(s) failed", report.failures.len())))
    }
}

/// Fetches the funding series.
async fn fetch_funding(command: &FundingCommand) -> Result<()> {
    let from = match &command.from_date {
        Some(value) => Range::parse(value, value)?.from,
        None => chrono::NaiveDate::from_ymd_opt(2019, 9, 8).expect("listing"),
    };
    let to = match &command.to_date {
        Some(value) => Range::parse(value, value)?.from,
        None => chrono::Utc::now().date_naive() + chrono::Duration::days(1),
    };
    let from_ms = build::day_start_ms(from);
    let to_ms = build::day_start_ms(to) + 86_400_000;
    let stamps = funding::fetch(&command.root, &command.symbol, from_ms, to_ms).await?;
    println!("{stamps} funding stamps, {} .. {}.", from, to);
    Ok(())
}

/// The commit stamped into the manifest, when the tool runs from a checkout.
fn tool_commit() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|commit| !commit.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn format_elapsed(duration: std::time::Duration) -> String {
    format_duration(duration)
}

/// Prints information about a collection of klines.
///
/// This function prints the number of elements, the start time, and the end time of the klines.
///
/// # Arguments
/// * `data` - A slice of klines implementing the `Kline` trait.
///
/// # Errors
/// Returns an error if the data slice is empty.
fn print_info<T: Kline>(data: &[T]) -> Result<()> {
    let first_k = data.first().ok_or(Error::MissingData)?;
    let last_k = data.last().ok_or(Error::MissingData)?;
    let n = data.len();
    let duration = last_k.close_time() - first_k.open_time();

    let days = duration.num_days();
    let hours = duration.num_hours() % 24;
    let minutes = duration.num_minutes() % 60;
    let duration = format!("{}D / {}H / {}m", separator(days, "_")?, separator(hours, "_")?, separator(minutes, "_")?);

    println!(
        "
========================
Number of elements: {n}
Duration: {duration}
It started from {open_time},
and ended at {close_time}.
========================
",
        open_time = first_k.open_time(),
        close_time = last_k.close_time()
    );

    Ok(())
}

/// Displays information about a JSON file containing klines.
///
/// This function reads the file, parses the klines, and prints information such as the number of elements,
/// the start time, and the end time.
///
/// # Arguments
/// * `cmd` - A reference to the info command configuration.
///
/// # Errors
/// Returns an error if the file cannot be read or parsed.
fn info(cmd: &InfoCommand) -> Result<()> {
    let filepath = &cmd.input_file;
    if let Ok(data) = read_data_from_file::<BinanceKline>(filepath) {
        return print_info(&data);
    }
    if let Ok(data) = read_data_from_file::<GateKline>(filepath) {
        return print_info(&data);
    }

    Err(Error::InvalidFile)
}

/// Fetches kline data from the selected exchange.
///
/// # Arguments
/// * `cmd` - A reference to the command configuration.
///
/// # Errors
/// Returns an error if the range is empty, a request fails after its retries,
/// or the output cannot be written.
async fn fetch(cmd: &Command) -> Result<()> {
    match cmd.market {
        Market::Binance => run_fetch::<_, BinanceKline>(&Binance::build(cmd), cmd).await,
        Market::Gate => run_fetch::<_, GateKline>(&Gate::build(cmd), cmd).await,
    }
}

async fn run_fetch<E, T>(endpoint: &E, cmd: &Command) -> Result<()>
where
    E: Endpoint,
    T: DeserializeOwned + Serialize + Kline + Send + 'static,
{
    let urls = endpoint.urls();
    if urls.is_empty() {
        return Err(Error::EmptyRange);
    }

    let progress = progress_bar(urls.len(), cmd)?;
    let downloader = Downloader::new(cmd.retries)?;
    let result = downloader
        .download::<T>(urls, usize::from(cmd.jobs), cmd.output_file.as_deref(), progress.as_ref())
        .await;

    let report = match result {
        Ok(report) => report,
        Err(error) => {
            if let Some(progress) = &progress {
                progress.abandon_with_message(error.to_string());
            }
            return Err(error);
        }
    };

    if let Some(progress) = &progress {
        progress.finish_and_clear();
    }

    println!(
        "Downloaded {} rows in {} requests over {}.",
        separator(report.rows, ",")?,
        separator(report.requests, ",")?,
        format_duration(report.elapsed),
    );

    if let (Some(first), Some(last)) = (report.first_open, report.last_close) {
        println!("Range: {} .. {}", first.to_rfc3339(), last.to_rfc3339());
    }

    if let Some(path) = &cmd.output_file {
        let bytes = std::fs::metadata(path)?.len();
        println!("Wrote {} ({} bytes).", path.display(), separator(bytes, ",")?);
    }

    Ok(())
}

fn progress_bar(len: usize, cmd: &Command) -> Result<Option<ProgressBar>> {
    if !cmd.verbose {
        return Ok(None);
    }

    let progress = ProgressBar::new(len as u64);
    let style = ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})\n{msg}")?
        .progress_chars("#>-");

    progress.set_style(style);
    progress.set_message(format!("{} jobs, {} retries", cmd.jobs, cmd.retries));

    Ok(Some(progress))
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds < 60.0 {
        format!("{seconds:.1}s")
    } else {
        format!("{}m {:02}s", duration.as_secs() / 60, duration.as_secs() % 60)
    }
}
