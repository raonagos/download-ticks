use std::fmt;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};

use crate::errors::{Error, Result};

/// Supported market to fetch the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Market {
    Gate,
    Binance,
}

impl fmt::Display for Market {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Market::Gate => write!(f, "gate"),
            Market::Binance => write!(f, "binance"),
        }
    }
}

/// Binance venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Venue {
    /// USD-M perpetual futures.
    Um,
    /// Spot.
    Spot,
}

/// Supported time intervals for klines.
#[derive(Debug, Clone, ValueEnum)]
pub enum Interval {
    /// 1 second
    #[value(name = "1s", alias = "s1")]
    S1,
    /// 1 minute
    #[value(name = "1m", alias = "m1")]
    M1,
    /// 3 minutes
    #[value(name = "3m", alias = "m3")]
    M3,
    /// 5 minutes
    #[value(name = "5m", alias = "m5")]
    M5,
    /// 15 minutes
    #[value(name = "15m", alias = "m15")]
    M15,
    /// 30 minutes
    #[value(name = "30m", alias = "m30")]
    M30,
    /// 1 hour
    #[value(name = "1h", alias = "h1")]
    H1,
    /// 2 hours
    #[value(name = "2h", alias = "h2")]
    H2,
    /// 4 hours
    #[value(name = "4h", alias = "h4")]
    H4,
    /// 6 hours
    #[value(name = "6h", alias = "h6")]
    H6,
    /// 8 hours
    #[value(name = "8h", alias = "h8")]
    H8,
    /// 12 hours
    #[value(name = "12h", alias = "h12")]
    H12,
    /// 1 day
    #[value(name = "1d", alias = "d1")]
    D1,
    /// 3 days
    #[value(name = "3d", alias = "d3")]
    D3,
    /// 1 week
    #[value(name = "1w", alias = "w1")]
    W1,
    /// 1 month
    #[value(name = "1M", alias = "mm1")]
    MM1,
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Interval::S1 => write!(f, "1s"),
            Interval::M1 => write!(f, "1m"),
            Interval::M3 => write!(f, "3m"),
            Interval::M5 => write!(f, "5m"),
            Interval::M15 => write!(f, "15m"),
            Interval::M30 => write!(f, "30m"),
            Interval::H1 => write!(f, "1h"),
            Interval::H2 => write!(f, "2h"),
            Interval::H4 => write!(f, "4h"),
            Interval::H6 => write!(f, "6h"),
            Interval::H8 => write!(f, "8h"),
            Interval::H12 => write!(f, "12h"),
            Interval::D1 => write!(f, "1d"),
            Interval::D3 => write!(f, "3d"),
            Interval::W1 => write!(f, "1w"),
            Interval::MM1 => write!(f, "1M"),
        }
    }
}

/// Command-line arguments for the program.
#[derive(Debug, Clone, Parser)]
#[command(
    version,
    about,
    long_about = "
This tool fetches historical candlestick (kline) data from exchanges like Binance.
You can specify a trading pair (e.g., BTCUSDT), time interval (e.g., 1m, 1h, 1d),
and optional start/end dates. The data is saved to a JSON file if --output-file is provided.

Requests run concurrently on the Tokio runtime, bounded by --jobs. Transient
failures (429, 408, 5xx, timeouts) are retried with exponential backoff.
Binance defaults to USD-M perpetual futures; pass --venue spot for spot klines.

## Supported Exchanges
- Binance
- Gate.io

Examples:
  Fetch 1-hour BTCUSDT klines for the latest 1000 hours:
    $ download-ticks fetch -s BTCUSDT -i 1h

  Fetch 1-minute BTCUSDT klines from Jan 1 to Mar 1, 2019, and save to output.json:
    $ download-ticks fetch -s BTCUSDT -i 1m --from-date '2019-01-01T00:00:00Z' --to-date '2019-03-01T00:00:00Z' -o output.json

  Fetch a full year with 16 concurrent requests:
    $ download-ticks fetch -s BTCUSDT -i 1m --from-date '2024-01-01T00:00:00Z' --to-date '2024-12-31T23:59:59Z' -j 16 -o btcusdt-1m-2024.json

  Show information of output.json file:
    $ download-ticks info -f output.json
"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn build() -> Result<Self> {
        let cmd = Self::parse();
        if let Commands::Fetch(cmd) = &cmd.command {
            if let (Some(from_date), Some(to_date)) = (cmd.from_date, cmd.to_date)
                && to_date < from_date
            {
                return Err(Error::InvalidDatetime);
            }
            if cmd.market == Market::Gate && cmd.venue.is_some() {
                return Err(Error::UnsupportedVenue);
            }
        }
        Ok(cmd)
    }
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    /// Fetch klines from an exchange.
    Fetch(Command),
    /// Display information about a JSON file containing klines.
    Info(InfoCommand),
}

/// Command-line arguments for fetching klines.
#[derive(Debug, Clone, Parser)]
pub struct Command {
    /// The market to fetch the data (e.g., binance).
    #[arg(short, long, default_value_t = Market::Binance)]
    pub market: Market,

    /// Binance venue. Defaults to USD-M futures; Gate.io is spot-only.
    #[arg(long, value_enum)]
    pub venue: Option<Venue>,

    /// The trading pair symbol (e.g., BTCUSDT, ETHUSDT).
    #[arg(short, long)]
    pub symbol: String,

    /// The time interval for klines.
    #[arg(short, long)]
    pub interval: Interval,

    /// Start date for fetching klines (UTC, RFC 3339 format).
    #[arg(short, long)]
    pub from_date: Option<DateTime<Utc>>,

    /// End date for fetching klines (UTC, RFC 3339 format).
    #[arg(short, long)]
    pub to_date: Option<DateTime<Utc>>,

    /// Output file path to save the klines in JSON format.
    #[arg(short, long)]
    pub output_file: Option<PathBuf>,

    /// Maximum number of requests in flight.
    #[arg(short = 'j', long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=64))]
    pub jobs: u16,

    /// Number of retries per request after the first attempt.
    #[arg(short = 'r', long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(0..=10))]
    pub retries: u16,

    /// Print progress status. Useful when a date range is given.
    #[arg(short, long)]
    pub verbose: bool,
}

/// Command-line arguments for displaying information about a JSON file.
#[derive(Debug, Clone, Parser)]
pub struct InfoCommand {
    /// Path to the JSON file containing klines.
    #[arg(short = 'f', long)]
    pub input_file: PathBuf,
}
