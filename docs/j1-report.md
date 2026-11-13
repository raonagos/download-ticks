# J1 — data spine, one verified month

**Date:** 2026-11-12. **Status:** pass on 2024-01, BTCUSDT UM.
**Scope:** `sync`, `funding`, `build`, `verify` — Binance Vision archives into
the coupled tape of `trad/docs/data-contract.md`.

## What was run

```sh
download-ticks sync    --symbol BTCUSDT --from-date 2024-01-01 --to-date 2024-01-31 --root data
download-ticks funding --symbol BTCUSDT --root data --from-date 2024-01-01 --to-date 2024-02-01
download-ticks build   --symbol BTCUSDT --from-date 2024-01-01 --to-date 2024-01-31 --root data
download-ticks verify  --root data --deep
```

## Result — 2024-01, BTCUSDT UM

| object | rows | bytes |
| --- | ---: | ---: |
| bookTicker | 725,011,366 | 6.28 GiB |
| aggTrades | 40,294,620 | 0.49 GiB |
| bookDepth | 89,190 (8,919 snapshots × 10 bands) | 0.01 GiB |
| klines 1m | 44,640 | 0.00 GiB |
| funding (REST) | 93 | 2 KiB |
| **tape** | **765,439,909 events** | **10.4 GiB zstd** |

`dataset_id: sha256:91db8fb0ce99f279bb49b1a064f13fd49828c8b0e11b2f69cd225d86c64311d0`

```text
ok   dataset_id: sha256:91db8fb0ce99f279bb49b1a064f13fd49828c8b0e11b2f69cd225d86c64311d0 (manifest says sha256:91db8fb0ce99f279bb49b1a064f13fd49828c8b0e11b2f69cd225d86c64311d0)
ok   objects/checksums: 125 objects
ok   tape/structure: 31 day files, 765439909 events
ok   tape/row-counts: match the manifest
ok   raw/row-counts: decoded counts match
ok   klines/grid: 0 gap run(s) listed, none filled
ok   gaps/archive-missing: no archive gaps
ok   funding/order: unique and ordered

dataset verified.
```

### Wall clock

| step | time |
| --- | ---: |
| sync 124 objects, 6.8 GiB | 28 min 34 s |
| build 31 days (decode, sort, zstd) | 15 min 49 s |
| verify --deep (765M events + re-decode) | 34 min 04 s |

The sync ran at ~2–4 MiB/s once two objects were in flight, far above the
570 KiB/s a single stream sustained; the month was budgeted at 3 h 45 and took
29 min. The full 2019–2026 backfill is ~560 GiB of `bookTicker`: feasible on
this link in ~2 days, not in an afternoon.

## Acceptance, one by one (data-contract §9)

| criterion | state |
| --- | --- |
| one month of BTCUSDT UM (klines + aggTrades + bookTicker + bookDepth) verifies | **pass** — above |
| the same month re-runs idempotently (zero new rows, zero new bytes) | **pass** — second sync reports `0 downloaded`, a second build reproduces the same `dataset_id` |
| a deliberate checksum flip is caught | **pass** — one byte flipped in `bookDepth`: quarantined, exit 1 |
| a deliberate gap is reported and not filled | **pass** — a root without `bookTicker` and 40 minutes removed from `klines` gives 2 gaps, `verify` confirms both and fills neither |
| the manifest hash is stable across a rebuild | **pass** — identical `dataset_id` |
| a `trad` loader can replay one day with no event after the decision time | **pass** — `trad` `src/tape.rs`, run on 2024-01-15: 8,984,201 events up to noon, last `ts` = cutoff − 1 ms |

## A finding: the archive is not time-ordered

The first three-day build failed on 2024-01-02 with a 23.6 h inversion. The
cause is in the object, not the reader: that day's `bookTicker` interleaves two
`update_id` streams (`3755…` and `3761…`), and the second carries the last
seconds of the day from line 361,605 onwards. Measured on the file: **180,805
adjacent inversions in 18,508,654 rows**, the worst 85,936,029 ms.

A reorder window cannot absorb that, so a day is decoded in parallel and sorted
by `(ts, kind, id)` before it is written. The tape is what the contract says it
is; the archive only supplied the rows. Cost: ~1.5 GiB of memory for a 20M-event
day, capped at 40M with a loud error.

## Limits

- One asset, one venue, one month. ETH/BNB, 2020–2025, and the other 11 months
  of 2024 are untouched.
- `verify` is single-threaded: 34 minutes for a month, so a year is ~7 hours.
  Parallelising it is the obvious next optimisation and does not change what is
  checked.
- Duplicate detection is a per-day id scan; `totals.duplicates` is written as 0
  and the check itself lives in `verify`.
- Funding is fetched by REST and is the only live call; it is pinned in the
  manifest by the file's SHA-256.

## Addendum — later the same day

`sync`, `build` and `verify` now use the machine's cores; the README's
throughput section has the flags and the bounds. Re-measured on the same
2024-01 dataset, same machine:

| run | this report | now |
| --- | ---: | ---: |
| `build`, 5 days | 2m 36s at 127% CPU | 1m 28s at 298% CPU |
| `verify --deep`, 5 days | 3m 58s at 99% CPU | 1m 45s at 381% CPU |
| `verify --deep`, 31 days | 34m 04s at 99% CPU | 6m 54s at 975% CPU |
| `sync`, 124 objects already present | 55 s at 12% CPU | 6.5 s at 75% CPU |

`dataset_id` is unchanged: the parallel build writes byte-identical tapes and
assembles the manifest in day order. The "verify is single-threaded" limit
above describes the binary this report measured, not the current one.

A fresh download does not improve, and the numbers say why: one day of objects
(304 MiB) took 8m 29s with two in flight and 9m 06s with twelve, both at
~0.6 MiB/s, and a bare `curl` to `data.binance.vision` and to Cloudflare both
measure ~0.6 MiB/s from this host. The link is the cap, so the 560 GiB
backfill is ~10 days of contiguous transfer, not the ~2 days estimated above
at 2–4 MiB/s. Fewer products, or a faster link, is the only lever there.
