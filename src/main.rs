//! A command-line tool to fetch and save candlestick (kline) data from exchanges.
//!
//! The downloader runs bounded concurrent requests on the Tokio runtime, streams
//! completed chunks to a JSON array in request order, retries transient errors,
//! and atomically publishes the output file.
//!
//! ## Usage
//! The tool is designed to be flexible and easy to use. See the `cli` module for
//! command-line options.

mod cli;
mod download;
mod errors;
mod market;
mod utils;

use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use serde::de::DeserializeOwned;

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
    }

    Ok(())
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
