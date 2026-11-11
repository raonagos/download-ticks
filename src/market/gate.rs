use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::{Endpoint, Kline};
use crate::{cli::Command, utils::split_intervals};

/// A wrapper for the Gate.io exchange configuration.
pub struct Gate<'b>(&'b Command);

impl<'b> Gate<'b> {
    const BASE_URL: &'static str = "https://api.gateio.ws/api/v4/spot/candlesticks";

    pub fn build(command: &'b Command) -> Self {
        Self(command)
    }
}

impl Endpoint for Gate<'_> {
    fn urls(&self) -> Vec<String> {
        let symbol = &self.0.symbol;
        let interval = &self.0.interval;
        let burl = Self::BASE_URL;
        let mut url = format!("{burl}?currency_pair={symbol}&interval={interval}&limit=1000");

        if let (Some(start), Some(end)) = (self.0.from_date, self.0.to_date) {
            let datetimes = split_intervals(start, end, interval);
            let urls = datetimes
                .iter()
                .map(|(start, end)| {
                    let _start = start.timestamp();
                    let _end = end.timestamp();
                    format!("{url}&from={_start}&to={_end}")
                })
                .collect::<Vec<_>>();
            return urls;
        }

        if let (Some(start), None) = (self.0.from_date, self.0.to_date) {
            url = format!("{url}&from={}", start.timestamp());
        } else if let (None, Some(end)) = (self.0.from_date, self.0.to_date) {
            url = format!("{url}&to={}", end.timestamp());
        }
        vec![url]
    }
}

/// A single candlestick (kline) from Gate.io.
///
/// As with Binance, the raw JSON row is kept verbatim. Gate only reports the
/// candle's open time, so the close time mirrors it.
#[derive(Debug, Clone)]
pub struct GateKline {
    open_time: DateTime<Utc>,
    raw: Vec<Value>,
}

impl<'de> Deserialize<'de> for GateKline {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Vec::<Value>::deserialize(deserializer)?;
        let open_time = raw
            .first()
            .and_then(timestamp_seconds)
            .and_then(DateTime::<Utc>::from_timestamp_secs)
            .ok_or_else(|| serde::de::Error::custom("invalid Gate.io kline timestamp"))?;

        Ok(Self { open_time, raw })
    }
}

impl Serialize for GateKline {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.raw.serialize(serializer)
    }
}

impl Kline for GateKline {
    fn open_time(&self) -> DateTime<Utc> {
        self.open_time
    }

    fn close_time(&self) -> DateTime<Utc> {
        self.open_time
    }
}

fn timestamp_seconds(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse::<i64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_preserves_the_exchange_row_verbatim() {
        let json = r#"["1704067200","1000.00000000","42320.5","42350.0","42200.0","42314.0","12.50000000","true"]"#;
        let kline: GateKline = serde_json::from_str(json).unwrap();

        assert_eq!(kline.open_time().timestamp(), 1_704_067_200);
        assert_eq!(kline.close_time(), kline.open_time());
        assert_eq!(serde_json::to_string(&kline).unwrap(), json);
    }
}
