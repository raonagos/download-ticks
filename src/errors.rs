use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

/// Custom error type for the application.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Check the first/last kline data.")]
    MissingData,

    #[error("Unable to read you input file. Make sure it is json kline valid data.")]
    InvalidFile,

    #[error("Invalid given datetime.")]
    InvalidDatetime,

    #[error("The requested range does not contain any interval.")]
    EmptyRange,

    #[error("--venue only applies to Binance; Gate.io is spot-only.")]
    UnsupportedVenue,

    #[error("Cannot derive an output file name from {0}.")]
    InvalidOutputPath(PathBuf),

    #[error("HTTP {status} while fetching {url}")]
    HttpStatus { status: reqwest::StatusCode, url: String },

    #[error("A download task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("{0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("{0}")]
    Indicatif(#[from] indicatif::style::TemplateError),

    #[error("{0}")]
    Str(#[from] std::str::Utf8Error),

    #[error("{0}")]
    Parse(#[from] std::num::ParseIntError),
}
