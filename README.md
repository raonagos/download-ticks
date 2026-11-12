# download-ticks

A command-line tool for downloading historical trading data from public
exchange APIs.

`download-ticks` has two modes. `fetch` pulls klines over a REST range and
stores the exchange's raw JSON rows in one array. `sync`/`build` pull the
public bulk archives (klines, aggTrades, bookTicker, bookDepth) plus the funding
series, verify them by checksum, and decode them into one canonical event tape
with a manifest that identifies the dataset. It is built for reliable bulk downloads:
bounded concurrency, retries with backoff, ordered streaming writes, and atomic
output.

## Features

- **Concurrent and bounded.** `--jobs` controls how many requests are in
  flight. Each request runs as a Tokio task on the multi-thread runtime.
- **Resilient.** Retries `408`, `429`, `5xx`, connection, and timeout failures
  with exponential backoff and jitter; honours `Retry-After`.
- **Bounded memory.** Completed chunks are written in request order as soon as
  the preceding chunk is available, so memory does not grow with the range.
- **Atomic.** The destination is written through a `.part` file and renamed
  only after the JSON array is closed. A failed run leaves no partial output.
- **Faithful.** Every row is the exchange's raw JSON array. Prices keep their
  original strings and precision; nothing is re-serialised or interpolated.
- **Portable.** `reqwest` uses `rustls-tls`; no OpenSSL headers are required.

## Supported data

| exchange | venue | klines |
| --- | --- | --- |
| Binance | USD-M perpetual futures (`--venue um`, default) | yes |
| Binance | spot (`--venue spot`) | yes |
| Gate.io | spot | yes |

More products (aggregated trades, book ticker/depth, funding rates) and bulk
archive sources are on the roadmap.

## Build

```sh
cargo build --release
cargo test --release
```

## Usage

```sh
# Latest 1,000 one-minute BTCUSDT USD-M candles (no output file).
download-ticks fetch --symbol BTCUSDT --interval 1m

# One full year of USD-M klines, 16 requests in flight.
download-ticks fetch --symbol BTCUSDT --interval 1m \
  --from-date 2024-01-01T00:00:00Z \
  --to-date 2024-12-31T23:59:59Z \
  --output-file btcusdt-1m-um-2024.json --jobs 16

# Spot klines instead of USD-M futures.
download-ticks fetch --venue spot --symbol BTCUSDT --interval 1h \
  --from-date 2024-01-01T00:00:00Z \
  --to-date 2024-01-02T00:00:00Z \
  --output-file btcusdt-1h-spot.json

# Gate.io spot klines.
download-ticks fetch --market gate --symbol BTC_USDT --interval 1m \
  --from-date 2024-01-15T00:00:00Z \
  --to-date 2024-01-15T23:59:59Z \
  --output-file btcusdt-1m-gate.json

# Inspect a downloaded file.
download-ticks info --input-file btcusdt-1m-um-2024.json
```

Gate.io serves a limited recent window (at most 10,000 points); requests for
older ranges are rejected by the API.

### Options

| flag | meaning |
| --- | --- |
| `-m, --market binance\|gate` | exchange family, default `binance` |
| `--venue um\|spot` | Binance venue, default `um`; Gate.io is spot-only |
| `-s, --symbol` | pair symbol (`BTCUSDT`, `BTC_USDT`, ...) |
| `-i, --interval` | kline interval (`1m`, `5m`, `1h`, `1d`, `1M`, ...) |
| `-f, --from-date` | inclusive start, RFC 3339 UTC |
| `-t, --to-date` | inclusive end, RFC 3339 UTC |
| `-o, --output-file` | JSON array destination; written atomically |
| `-j, --jobs N` | maximum requests in flight, default 8 |
| `-r, --retries N` | retries per request after the first attempt, default 3 |
| `-v, --verbose` | progress bar |

Without a date range the exchange's default page (up to 1,000 candles) is
fetched. Without `--output-file` the data is downloaded and counted but not
stored.

## Archive datasets

Binance publishes daily ZIPs for USD-M futures with a `.CHECKSUM` sibling. The
archive path is the only supported way to build a dataset large enough for
research, and it is the input to `trad`.

```sh
# 1. Raw objects: verified against the provider's CHECKSUM, atomic, idempotent.
download-ticks sync --symbol BTCUSDT --from-date 2024-01-15 --to-date 2024-01-15 --root data

# 2. Funding stamps (REST, no archive): normalised to ts_ms,rate.
download-ticks funding --symbol BTCUSDT --root data --from-date 2024-01-01 --to-date 2024-02-01

# 3. Decode raw into the tape and write data/manifest.json.
download-ticks build --symbol BTCUSDT --from-date 2024-01-15 --to-date 2024-01-15 --root data

# 4. The nine checks of the data contract.
download-ticks verify --root data --deep
```

Layout under `--root`:

```text
data/
  manifest.json
  raw/binance/um/BTCUSDT/{klines/1m,aggTrades,bookTicker,bookDepth}/*.zip
  tape/binance/um/BTCUSDT/YYYY-MM-DD.jsonl.zst
  funding/binance/um/BTCUSDT.csv
```

Guarantees:

- **Verified before decode.** A downloaded object is checked against the
  provider's SHA-256 before it is renamed into place. A mismatch is quarantined
  (`*.zip.quarantine-<hash16>`) and the run exits non-zero.
- **Idempotent.** An object already present with a matching hash is not
  downloaded again; re-running `build` reproduces the same `dataset_id`.
- **Atomic.** Every file is written to `.part` and renamed, so a crash never
  leaves a half object or a half manifest.
- **No filled gaps.** A missing day, or a missing minute, is listed in the
  manifest and stays missing in the tape.
- **Bounded memory.** Decoding streams each object on its own thread behind a
  bounded channel and merges the day with a k-way heap; a 20-million-row
  `bookTicker` day never sits in memory.

The tape is one JSON object per line, sorted by `(ts, kind, id)`, zstd
compressed. Kinds: `trade`, `book_ticker`, `book_depth`, `kline`, `funding`.
The manifest records every object (path, SHA-256, rows, timestamp bounds), the
gap list, and `dataset_id` — the SHA-256 of the manifest with `dataset_id` and
`created_utc` blanked, so the identity survives a rebuild.

`verify` checks the checksums, the manifest row counts, the tape structure and
per-kind invariants (non-crossed book, ten depth bands, positive trade sizes,
monotone ids, 1m kline grid, ordered funding), that every kline hole is listed
and none is filled, and that `dataset_id` recomputes. `--deep` also decodes
every raw object again and compares its row count.

## Output format

Each output file is one JSON array. Every element is the exchange's raw kline
row:

```json
[
  [1704067200000, "42314.00000000", "42350.00000000", "42200.00000000", "42320.00000000", "10.50000000", 1704067259999, "444000.00000000", 120, "5.00000000", "211000.00000000", "0"]
]
```

- Binance rows have 12 columns: `open_time, open, high, low, close, volume,
  close_time, quote_volume, trades, taker_buy_base, taker_buy_quote, ignore`.
- Gate.io rows have 8 columns: `time, quote_volume, close, high, low, open,
  base_volume, window`.

The `info` command prints the row count and covered range without rewriting the
file.

## Concurrency and durability

`fetch` uses one Tokio task per URL and keeps a sliding window of at most
`--jobs` tasks in flight. Concurrency is bounded by construction (no shared
semaphore), the work is spread over the runtime's worker threads, and dropping
the `JoinSet` on failure aborts every outstanding request.

Transient failures are retried with exponential backoff and jitter; a
`Retry-After` response header is honoured. Completed chunks are written in
request order, and the destination is renamed into place only after the JSON
array is closed. A failed request or a returned error therefore leaves no
partial dataset at the requested path; an abrupt kill may leave a `.part` file,
which is never mistaken for the final dataset.

There is no resume or manifest yet: a run always starts from the beginning of
the requested range. Gaps in the exchange data are preserved as gaps; they are
never filled or interpolated.

## Roadmap

- ~~Bulk archives, more products, dataset manifest with verification,
  resume/idempotency~~ — delivered by `sync`, `build`, and `verify`.
- Monthly archive objects (daily is used today: one request per day and
  product).
- More exchanges and venues.

Reference notes for archive layouts and schemas are in
[`docs/data-sources.md`](docs/data-sources.md).

## Non-goals

- No live trading, exchange keys, or order endpoints.
- No interpolation, synthetic bars, or price "cleanup".
- No upload of market data.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --release
```

## Releasing

Releases are tag-driven. Push a tag `vX.Y.Z` that matches `Cargo.toml`; the
`Publish` workflow runs the checks and publishes to crates.io. See
[RELEASING.md](RELEASING.md) for the one-time trusted-publishing setup.

## Contributing

Issues and pull requests are welcome.

## License

MIT — see [LICENSE](LICENSE).
