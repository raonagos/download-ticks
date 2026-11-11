# Data sources — Binance public archives

**Status:** reference for the `download-ticks` rewrite, 2026-11-10.
**Scope:** URL layout, file schemas, coverage, and the traps that matter for a
no-lookahead tape.

The client must verify the object listing before hardcoding a filename. The
patterns below are the stable shape; the exact columns are read from the file
header and recorded in the manifest.

## 1. Base

```text
https://data.binance.vision/data/
```

Objects are S3-backed and public. Each ZIP has a sibling `.CHECKSUM` file.
Daily and monthly variants exist; daily is the unit for a rebuild, monthly is
an optimisation for old ranges.

## 2. USD-M futures

Base: `data/futures/um/`.

| product | daily path |
| --- | --- |
| klines | `daily/klines/<SYMBOL>/<INTERVAL>/<SYMBOL>-<INTERVAL>-<YYYY-MM-DD>.zip` |
| aggTrades | `daily/aggTrades/<SYMBOL>/<SYMBOL>-aggTrades-<YYYY-MM-DD>.zip` |
| bookTicker | `daily/bookTicker/<SYMBOL>/<SYMBOL>-bookTicker-<YYYY-MM-DD>.zip` |
| bookDepth | `daily/bookDepth/<SYMBOL>/<SYMBOL>-bookDepth-<YYYY-MM-DD>.zip` |

Monthly replaces `daily/` with `monthly/` and the day with `YYYY-MM`.

Example:

```text
https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-15.zip
https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-15.zip
https://data.binance.vision/data/futures/um/daily/bookTicker/BTCUSDT/BTCUSDT-bookTicker-2024-01-15.zip
https://data.binance.vision/data/futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2024-01-15.zip
```

### 2.1 Schemas

**Klines** (USD-M): the standard 12-column Binance row.

```text
open_time, open, high, low, close, volume, close_time,
quote_volume, trades, taker_buy_base, taker_buy_quote, ignore
```

**aggTrades** (USD-M), header verified on a 2024 sample:

```text
agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
```

`transact_time` is milliseconds. The aggressor side is `is_buyer_maker == false`
→ buy, `true` → sell.

**bookTicker** (USD-M): event-level best bid/ask. The decoder reads the header;
the expected fields are an update id, best bid price/size, best ask price/size,
and transaction/event times. It is a **one-level** book, not a ladder.

**bookDepth** (USD-M), verified on `BTCUSDT-bookDepth-2024-01-15.zip`:

```text
timestamp,percentage,depth,notional
2024-01-15 00:00:12,-5,12251.47300000,499565539.44126000
2024-01-15 00:00:12,-4,10416.99100000,426495460.10329000
...
```

- Ten bands: −5, −4, −3, −2, −1, +1, +2, +3, +4, +5 percent from the mid.
- `depth` is base volume, `notional` is quote value.
- 2,881 snapshots on the sampled day, i.e. roughly every 30 seconds (the
  timestamps are not exactly on a 30 s grid).
- This is **aggregate depth**, not a top-20 price ladder. It supports depth
  shape, asymmetry, and a conservative impact model. It does not support
  level-by-level queue simulation.

## 3. Spot

Base: `data/spot/`.

| product | daily path |
| --- | --- |
| klines | `daily/klines/<SYMBOL>/<INTERVAL>/<SYMBOL>-<INTERVAL>-<YYYY-MM-DD>.zip` |
| aggTrades | `daily/aggTrades/<SYMBOL>/<SYMBOL>-aggTrades-<YYYY-MM-DD>.zip` |
| bookTicker | `daily/bookTicker/<SYMBOL>/<SYMBOL>-bookTicker-<YYYY-MM-DD>.zip` |

Spot has **no** `bookDepth` archive. A full spot ladder would require a live
recorder. Spot timestamps switch to microseconds from 2025-01-01; the decoder
must detect the unit.

## 4. Funding

Funding is REST, not an archive:

```text
GET https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=<ms>&limit=1000
```

Paginate by `fundingTime + 1`, cap at the API limit, deduplicate by stamp. Rates
are fractions, not percent.

## 5. Coverage

| symbol | USD-M listing | first bookDepth |
| --- | --- | --- |
| BTCUSDT | 2019-09 | listing |
| ETHUSDT | 2019-11 | listing |
| BNBUSDT | 2020-02 | listing |

There is no order-book history before the listing. A 2018 in-sample window can
use spot klines only; it cannot be coupled to L2.

## 6. Checksums

Every object has a `.CHECKSUM` sibling. Verify before decode:

```sh
sha256sum -c BTCUSDT-1m-2024-01-15.zip.CHECKSUM
```

The manifest stores the SHA-256 of the stored bytes and the provider checksum
status. A mismatch is quarantined, never overwritten.

## 7. Traps

- **Do not assume a ladder.** `bookDepth` is aggregate bands. A fill model that
  walks twenty levels is fiction on this data.
- **Do not use `close_time` as the event time.** A kline is not readable until
  the next bar opens; emit it at its close.
- **Do not interpolate.** A missing day is a gap. Report it.
- **Do not sort by a derived index.** Sort by exchange time and update id.
- **Do not trust a partial ZIP.** HTTP 200 is not a complete object; verify the
  checksum and the central directory.
- **Do not fetch a year through REST when Vision has the archive.** The REST
  path is for recent days only.
- **Do not reuse a stale book.** A decision uses the latest book event with
  `time <= t`; a book event after `t` is future information.

## 8. Acceptance sample

A one-day smoke test is accepted when:

1. all four objects download and their checksums match;
2. the klines decode to 1,440 rows (or the exact expected count for the day);
3. aggTrades decode with positive price/size and a valid aggressor side;
4. bookTicker is non-crossed and its update ids are monotone;
5. bookDepth has ten bands per snapshot and 2,800–2,900 snapshots;
6. the tape replay at a chosen `t` sees no event with `time > t`;
7. a deliberate one-byte corruption is caught by `verify`.
