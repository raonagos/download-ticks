use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::{Endpoint, Kline};
use crate::cli::{Command, Venue};
use crate::utils::split_intervals;

/// A wrapper for the Binance exchange configuration.
pub struct Binance<'b>(&'b Command);

impl<'b> Binance<'b> {
    const SPOT_URL: &'static str = "https://api.binance.com/api/v3/klines";
    const UM_URL: &'static str = "https://fapi.binance.com/fapi/v1/klines";

    pub fn build(command: &'b Command) -> Self {
        Self(command)
    }

    fn base_url(&self) -> &'static str {
        match self.0.venue.unwrap_or(Venue::Um) {
            Venue::Um => Self::UM_URL,
            Venue::Spot => Self::SPOT_URL,
        }
    }
}

impl Endpoint for Binance<'_> {
    fn urls(&self) -> Vec<String> {
        let symbol = &self.0.symbol;
        let interval = &self.0.interval;
        let burl = self.base_url();
        let mut url = format!("{burl}?symbol={symbol}&interval={interval}&limit=1000");

        if let (Some(start), Some(end)) = (self.0.from_date, self.0.to_date) {
            let datetimes = split_intervals(start, end, interval);
            let urls = datetimes
                .iter()
                .map(|(start, end)| {
                    let _start = start.timestamp_millis();
                    let _end = end.timestamp_millis();
                    format!("{url}&startTime={_start}&endTime={_end}")
                })
                .collect::<Vec<_>>();
            return urls;
        }

        if let (Some(start), None) = (self.0.from_date, self.0.to_date) {
            url = format!("{url}&startTime={}", start.timestamp_millis());
        } else if let (None, Some(end)) = (self.0.from_date, self.0.to_date) {
            url = format!("{url}&endTime={}", end.timestamp_millis());
        }
        vec![url]
    }
}

/// A single candlestick (kline) from Binance.
///
/// The raw JSON row is kept verbatim so that the stored dataset preserves the
/// exchange's own strings and precision. Only the two timestamps needed for
/// progress reporting and `info` are parsed.
#[derive(Debug, Clone)]
pub struct BinanceKline {
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    raw: Vec<Value>,
}

impl<'de> Deserialize<'de> for BinanceKline {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Vec::<Value>::deserialize(deserializer)?;
        let open_time = raw
            .first()
            .and_then(timestamp_millis)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .ok_or_else(|| serde::de::Error::custom("invalid Binance kline open time"))?;
        let close_time = raw
            .get(6)
            .and_then(timestamp_millis)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .ok_or_else(|| serde::de::Error::custom("invalid Binance kline close time"))?;

        Ok(Self { open_time, close_time, raw })
    }
}

impl Serialize for BinanceKline {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.raw.serialize(serializer)
    }
}

impl Kline for BinanceKline {
    fn open_time(&self) -> DateTime<Utc> {
        self.open_time
    }

    fn close_time(&self) -> DateTime<Utc> {
        self.close_time
    }
}

fn timestamp_millis(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse::<i64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_preserves_the_exchange_row_verbatim() {
        let json = r#"[1704067200000,"42314.00000000","42350.00000000","42200.00000000","42320.00000000","10.50000000",1704067259999,"444000.00000000",120,"5.00000000","211000.00000000","0"]"#;
        let kline: BinanceKline = serde_json::from_str(json).unwrap();

        assert_eq!(kline.open_time().timestamp_millis(), 1_704_067_200_000);
        assert_eq!(kline.close_time().timestamp_millis(), 1_704_067_259_999);
        assert_eq!(serde_json::to_string(&kline).unwrap(), json);
    }
}
