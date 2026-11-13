//! Helpers shared by the tests: a scratch dataset root and one day of klines.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::NaiveDate;

use crate::archive::Product;
use crate::build::day_start_ms;

/// An empty directory under the temporary directory, recreated for each test.
pub fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("download-ticks-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

/// One day of 1m klines at the archive's path: a header and `minutes` rows.
pub fn write_klines(root: &Path, date: NaiveDate, minutes: usize) {
    let day = date.format("%Y-%m-%d");
    let path = Product::Klines.raw_path(root, "BTCUSDT", date, "1m");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut csv = String::from("open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote,ignore\n");
    let start = day_start_ms(date);
    for minute in 0..minutes {
        let open = start + minute as i64 * 60_000;
        csv.push_str(&format!("{open},100.0,101.0,99.0,100.5,10.0,{},1000.0,5,4.0,400.0,0\n", open + 59_999));
    }
    let file = fs::File::create(&path).unwrap();
    let mut writer = zip::ZipWriter::new(file);
    writer
        .start_file(format!("BTCUSDT-1m-{day}.csv"), zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(csv.as_bytes()).unwrap();
    writer.finish().unwrap();
}
