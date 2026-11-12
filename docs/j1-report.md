# J1 — data spine, first slice

**Date:** 2026-11-12. **Status:** one day verified end to end; the month is the
next run, not a claim.
**Scope:** `sync`, `build`, `verify`, `funding` — Binance Vision archives into
the coupled tape of `trad/docs/data-contract.md`.

## What was run

```sh
download-ticks sync   --symbol BTCUSDT --from-date 2024-01-15 --to-date 2024-01-15 --root data
download-ticks funding --symbol BTCUSDT --root data --from-date 2024-01-01 --to-date 2024-02-01
download-ticks build   --symbol BTCUSDT --from-date 2024-01-15 --to-date 2024-01-15 --root data
download-ticks verify  --root data --deep
```

## Result — 2024-01-15, BTCUSDT UM

| object | rows | bytes |
| --- | ---: | ---: |
| klines 1m | 1,440 | 62,120 |
| aggTrades | 1,036,190 | 13,510,974 |
| bookTicker | 19,881,490 | 188,017,444 |
| bookDepth | 2,880 (288 snapshots × 10 bands) | 477,151 |
| funding (REST, in range) | 3 | 2,084 |
| **tape** | **20,922,003 events** | **301.3 MiB zstd** |

`dataset_id: sha256:297934394e9b86a61c49fe5181fcfb740c43cb8d40216cae8e579da8cc7917c2`

```text
ok   dataset_id: sha256:297934394e9b86a61c49fe5181fcfb740c43cb8d40216cae8e579da8cc7917c2 (manifest says sha256:297934394e9b86a61c49fe5181fcfb740c43cb8d40216cae8e579da8cc7917c2)
ok   objects/checksums: 5 objects
ok   tape/structure: 1 day files, 20922003 events
ok   tape/row-counts: match the manifest
ok   raw/row-counts: decoded counts match
ok   klines/grid: 0 gap run(s) listed, none filled
ok   gaps/archive-missing: no archive gaps
ok   funding/order: unique and ordered

dataset verified.
```

Timings on one core-worthy laptop: sync 6 min 01 s for 192.7 MiB; build 21–22 s
for the day (decode, merge, zstd); `verify --deep` 37 s (re-decodes the raw).

## Acceptance, one by one

| criterion (data-contract §9) | state |
| --- | --- |
| one month of BTCUSDT UM verifies | **not yet** — one day does; the month is a 3.5 h download |
| the same month re-runs idempotently (zero new rows, zero new bytes) | **pass** — second sync: `0 downloaded, 4 already present, 192.7 MiB in 1.2s`; rebuild yields the same `dataset_id` |
| a deliberate checksum flip is caught | **pass** — one byte flipped in `bookDepth`: `Quarantined`, exit 1, `*.quarantine-c69b4ae2cb52e775` |
| a deliberate gap is reported and not filled | **pass** — a root without `bookTicker` and with 40 minutes removed from `klines` yields 2 gaps; `verify` reports `1 gap run(s) listed, none filled` and `1 archive gap(s) confirmed absent` |
| the manifest hash is stable across a rebuild | **pass** — identical `dataset_id` across rebuilds |
| a `trad` loader can replay one day with no event after the decision time | **open** — the loader is J1's last item and lives in `trad` |

## Cost of the month (measured, not guessed)

The link sustains ~570 KiB/s to `data.binance.vision` (and to Cloudflare; no
proxy in the path). Object sizes for 2024-01, BTCUSDT UM:

| object | month size | at 570 KiB/s |
| --- | ---: | ---: |
| bookTicker | 6.95 GiB | ~3 h 30 |
| aggTrades | 522 MiB | ~15 min |
| klines 1m | 1.9 MiB | seconds |
| bookDepth | 477 KiB/day (no monthly object) | ~15 MiB, seconds |

So one month ≈ 7.5 GiB ≈ **3 h 45** of download, plus ~11 min of build and
~20 min of `verify --deep`. The full 2019–2026 backfill of `bookTicker` is
~560 GiB, i.e. weeks at this rate: it needs a faster link or a smaller product
set before it is scheduled.

## Limits

- One asset, one venue, one day. ETH/BNB and the month are untouched.
- `bookDepth` has no monthly object, so the sync is daily per product.
- The `trad` replay loader does not exist yet; the tape is verified but nothing
  in `trad` reads it.
- Duplicate detection is `verify`'s monotone-id check, not a manifest count;
  `totals.duplicates` stays 0 by construction.
