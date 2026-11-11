use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use serde::de::DeserializeOwned;
use serde_json::from_reader;

use crate::{Error, Interval, Result};

/// Splits a time range into intervals suitable for Binance's API (max 999 candles per request).
///
/// # Arguments
/// * `start` - Start date of the range.
/// * `end` - End date of the range.
/// * `interval` - The time interval (m1, h1, d1).
///
/// # Returns
/// A vector of tuples `(start, end)` representing the split intervals.
pub fn split_intervals(start: DateTime<Utc>, end: DateTime<Utc>, interval: &Interval) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let mut intervals = Vec::new();
    let mut current_start = start;

    // maximum duration for 999 candles, based on the interval.
    // also return the interval duration to increment `current_start`.
    let (max_duration, plus_duration) = match interval {
        Interval::S1 => (Duration::seconds(999), Duration::seconds(1)),
        Interval::M1 => (Duration::minutes(999), Duration::minutes(1)),
        Interval::M3 => (Duration::minutes(999), Duration::minutes(3)),
        Interval::M5 => (Duration::minutes(999), Duration::minutes(5)),
        Interval::M15 => (Duration::minutes(999), Duration::minutes(15)),
        Interval::M30 => (Duration::minutes(999), Duration::minutes(30)),
        Interval::H1 => (Duration::hours(999), Duration::hours(1)),
        Interval::H2 => (Duration::hours(999), Duration::hours(2)),
        Interval::H4 => (Duration::hours(999), Duration::hours(4)),
        Interval::H6 => (Duration::hours(999), Duration::hours(6)),
        Interval::H8 => (Duration::hours(999), Duration::hours(8)),
        Interval::H12 => (Duration::hours(999), Duration::hours(12)),
        Interval::D1 => (Duration::days(999), Duration::days(1)),
        Interval::D3 => (Duration::days(999), Duration::days(3)),
        Interval::W1 => (Duration::weeks(999), Duration::weeks(1)),
        Interval::MM1 => (Duration::weeks(999), Duration::weeks(4)),
    };

    while current_start < end {
        let current_end = std::cmp::min(current_start + max_duration, end);
        intervals.push((current_start, current_end));
        current_start = current_end + plus_duration; // avoid overlapping
    }

    intervals
}

/// Reads candlestick data from a file containing serialized Kline data.
pub fn read_data_from_file<T>(path: &Path) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    from_reader(reader).map_err(Error::from)
}

/// Number separator
pub fn separator<T: ToString>(num: T, sep: &str) -> Result<String> {
    num.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<std::result::Result<Vec<&str>, _>>()
        .map(|num_sep| num_sep.join(sep))
        .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn split_intervals_covers_the_range_without_overlap() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let chunks = split_intervals(start, end, &Interval::M1);

        assert_eq!(chunks.first().unwrap().0, start);
        assert_eq!(chunks.last().unwrap().1, end);
        for pair in chunks.windows(2) {
            assert_eq!(pair[1].0, pair[0].1 + Duration::minutes(1));
        }
        for (chunk_start, chunk_end) in &chunks {
            assert!(chunk_start < chunk_end);
        }
    }

    #[test]
    fn split_intervals_returns_nothing_for_an_empty_range() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        assert!(split_intervals(start, start, &Interval::M1).is_empty());
    }

    #[test]
    fn separator_groups_digits() {
        assert_eq!(separator(1_234_567u64, ",").unwrap(), "1,234,567");
        assert_eq!(separator(42u64, ",").unwrap(), "42");
    }
}
