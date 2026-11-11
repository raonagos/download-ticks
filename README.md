# download-ticks — the data spine

`download-ticks` is the only way market data enters System One. It fetches,
verifies, and normalises the coupled tape that `trad` trains and scores on:

```text
1-minute klines + aggTrades + bookTicker/bookDepth + funding
```

It is resumable and idempotent. It never interpolates a gap, never silently
drops a failed chunk, and writes a manifest that identifies the dataset.

> **Status 2026-11-11.** The current code fetches klines only, through REST.
> The TLS build uses `rustls` and no longer needs OpenSSL. Downloads run as a
> bounded `JoinSet` (one Tokio task per request), retry transient failures with
> exponential backoff, stream chunks to the output in request order, and publish
> the file with an atomic rename. There is still no checksum/resume/verify path
> and no Vision archive client. This README defines the target. The rewrite
> lands in this repository's PR, one step at a time.

## Why it exists

The previous corpus was lost. A trading result is only reproducible if its data
has an identity: source, range, checksums, row counts, gaps, and the tool commit
that produced it. `download-ticks` owns that identity. No ad-hoc `curl`, no
throwaway script, no vendor upload.

The primary venue is **Binance USD-M perpetual** because Binance Vision
archives `bookTicker` (event-level best bid/ask) and `bookDepth` (aggregate
depth at ±1..5% every ~30 s) for it. It does not archive a full top-20 ladder.
Spot has `bookTicker` only.

## Build

```sh
cargo build --release
cargo test  --release
```

`reqwest` uses `rustls-tls`; no OpenSSL headers are required.

## Commands

The CLI is organised around a fetch, a verify, and a manifest.

### Current CLI

The current build implements kline `fetch` and `info` over REST, for Binance
(spot or USD-M) and Gate.io (spot). The `--kind`, `--source`, `--resume`,
`--verify`, and `--out` forms in the target examples below are not parsed yet.

```sh
# One day of USD-M perpetual 1-minute klines, 16 requests in flight.
download-ticks fetch --market binance --venue um --symbol BTCUSDT \
  --interval 1m --from-date 2024-01-15T00:00:00Z \
  --to-date 2024-01-15T23:59:59Z --output-file btcusdt-1m-2024-01-15.json \
  --jobs 16

# Inspect the stored JSON array.
download-ticks info --input-file btcusdt-1m-2024-01-15.json
```

The stored rows are the exchange's raw JSON arrays, not a re-serialised struct:
prices keep their original strings and precision.

| flag | meaning |
| --- | --- |
| `--market binance\|gate` | exchange family |
| `--venue um\|spot` | Binance venue, default `um`; Gate.io is spot-only |
| `--symbol`, `--interval` | pair and kline interval (`1m`, `1h`, `1d`, ...) |
| `--from-date`, `--to-date` | inclusive RFC 3339 range, UTC |
| `--output-file` | JSON array destination; written atomically |
| `--jobs N` | bounded concurrency, default 8 |
| `--retries N` | retries per request after the first attempt, default 3 |
| `--verbose` | progress bar |

### Target CLI

The forms below are the target interface for the full data spine.

#### Fetch

```sh
# 1-minute klines for one day
download-ticks fetch --market um --symbol BTCUSDT --kind klines \
  --interval 1m --from 2024-01-15 --to 2024-01-15 --out data

# aggregated trades for the same day
download-ticks fetch --market um --symbol BTCUSDT --kind aggTrades \
  --from 2024-01-15 --to 2024-01-15 --out data

# aggregate depth bands (~30 s)
download-ticks fetch --market um --symbol BTCUSDT --kind bookDepth \
  --from 2024-01-15 --to 2024-01-15 --out data

# best bid/ask updates
download-ticks fetch --market um --symbol BTCUSDT --kind bookTicker \
  --from 2024-01-15 --to 2024-01-15 --out data

# funding stamps
download-ticks fetch --market um --symbol BTCUSDT --kind funding \
  --from 2019-09-08 --to 2026-11-01 --out data
```

Target options:

| flag | meaning |
| --- | --- |
| `--source auto\|vision\|rest` | `auto` uses Vision for closed days, REST for recent |
| `--from`, `--to` | inclusive date range, UTC |
| `--resume` | skip objects already verified in the manifest |
| `--verify` | checksum and decode each object after download |
| `--best-effort` | record failures and exit 0; default is fail-fast |
| `--out DIR` | dataset root |

### Concurrency and durability

`fetch` uses one Tokio task per URL and keeps a sliding window of at most
`--jobs` tasks in flight. Concurrency is bounded by construction (no shared
semaphore), the work is spread over the runtime's worker threads, and dropping
the `JoinSet` on failure aborts every outstanding request.

Transient failures (408, 429, 5xx, connection and timeout errors) are retried
with exponential backoff and jitter; `Retry-After` is honoured. Chunks are
written in request order as soon as the preceding chunk is available, so memory
does not grow with the size of the range. The output is a `.part` file while it
is being written and is renamed into place only after the JSON array is closed.

### Verify

```sh
download-ticks verify --manifest data/manifest.json
```

Checks checksums, row counts, monotonic timestamps, duplicate ids, crossed
books, kline grid gaps, funding stamps, and the recomputed `dataset_id`. Exits
non-zero on any error.

### Info

```sh
download-ticks info --manifest data/manifest.json
```

Prints range, row counts, bytes, gaps, duplicates, and the dataset id.

## Output layout

```text
data/
  manifest.json
  raw/binance/um/BTCUSDT/
    klines/1m/BTCUSDT-1m-2024-01-15.zip
    aggTrades/BTCUSDT-aggTrades-2024-01-15.zip
    bookTicker/BTCUSDT-bookTicker-2024-01-15.zip
    bookDepth/BTCUSDT-bookDepth-2024-01-15.zip
  tape/binance/um/BTCUSDT/2024-01-15.jsonl.zst
  funding/binance/um/BTCUSDT.csv
```

`raw/` is immutable and checksummed. `tape/` is derived from raw and can be
rebuilt. `manifest.json` is the dataset identity.

The manifest schema and the canonical event format are in the `trad` repository:
[`docs/data-contract.md`](https://github.com/raonagos/trad/blob/main/docs/data-contract.md).

## Guarantees

- **Resumable.** A run interrupted at any object resumes from the verified
  manifest and does not re-fetch completed bytes.
- **Idempotent.** A second run over the same range adds zero rows and zero
  bytes; a checksum mismatch is quarantined, never overwritten.
- **Atomic.** `.part` file, then rename. No half-written object is visible.
- **Checksummed.** Provider `CHECKSUM` verified before decode; the manifest
  records the SHA-256 of the stored bytes.
- **No silent gaps.** A missing object or a missing interval is a row in
  `gaps[]`, not a skipped chunk.
- **No interpolation.** A gap is data. The consumer decides what to do.
- **Polite.** Bounded concurrency, retry with backoff on 429/5xx, no retry on
  4xx, per-host rate limit.
- **Deterministic.** The same inputs and tool commit rebuild the same event
  order and the same `dataset_id`.

## Coverage

| product | spot | USD-M futures | note |
| --- | --- | --- | --- |
| klines | yes | yes | daily and monthly |
| aggTrades | yes | yes | daily and monthly |
| bookTicker | yes | yes | best bid/ask only |
| bookDepth | no | yes | aggregate ±1..5% bands, ~30 s |
| funding | no | yes | 8-hour stamps |

USD-M listing dates: BTCUSDT 2019-09, ETHUSDT 2019-11, BNBUSDT 2020-02. There is
no L2 before those dates. Historical full-depth spot data is not publicly
archived; a live recorder is the only way to start collecting it.

Source URL patterns and file formats: [`docs/data-sources.md`](docs/data-sources.md).

## Roadmap

1. **Build and CLI skeleton** — done: `rustls`, `fetch`/`info`, bounded and
   retrying REST downloader. `verify` and the manifest are still pending.
2. **Vision client** — daily/monthly objects, checksum, range planning,
   resumable download.
3. **Decoders** — klines, aggTrades, bookTicker, bookDepth, funding; canonical
   event tape; no-lookahead ordering. `bookDepth` is aggregate bands, not a
   ladder.
4. **Verify** — all checks in the data contract, including a deliberate
   corruption test.
5. **Backfill** — BTC/ETH/BNB UM, data card, one-month acceptance before the
   full range.
6. **Live recorder (optional)** — only if a full ladder or spot depth is needed;
   not part of the first milestone.

## Non-goals

- No trading, no keys, no order endpoints.
- No data vendor with a key.
- No interpolation, no synthetic bars, no “cleanup” that changes a price.
- No upload of market data.

## Development

```sh
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test --release
```

A change to a stored format or a manifest field is a breaking change: bump the
schema and say so in the pull request.
