mod binance;
mod gate;

pub use binance::*;
pub use gate::*;

use chrono::{DateTime, Utc};

/// Trait for kline data.
pub trait Kline {
    fn open_time(&self) -> DateTime<Utc>;
    fn close_time(&self) -> DateTime<Utc>;
}

/// Trait to compute urls.
pub trait Endpoint {
    fn urls(&self) -> Vec<String>;
}
