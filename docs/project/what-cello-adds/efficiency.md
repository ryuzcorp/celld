---
title: Efficiency and cost
description: Fewer bucket requests, less memory, and the s3bench harness over upstream denoland/celld.
---

Patches that cut bucket request counts, memory, and latency — with the
measurements to prove them, re-measurable in `tools/s3bench/`. Each
entry names its fork patch number. Bucket-posture group commit has its
own [Fleet](fleet-durability.md) section.

## Fewer bucket requests

- `0054` — Eviction skips visibility LISTs it can prove locally (see
  [Storage](storage.md)). Measured on 200 short-lived Durable Objects
  (6 writes each, `CELLO_DURABILITY=bucket`): 12 Class-A requests per
  cell before, 9 after.
- `0055` — A lone fleet node slows its posture-repair scan: while a
  `maintain` pass finds no eligible peer, the wait before the next pass
  doubles from one second up to the 30 s maintenance tick. The flat
  retry cost a lone node about 2.6M LIST plus 2.6M GET a month doing
  nothing. A degraded ensemble that lost all its followers still
  retries at once, so it re-recruits the moment one returns.
- `0056` — Concurrent bucket-posture writes on one cell share ownership
  reads (see [Storage](storage.md)). Measured on one hot cell with 16
  concurrent writers: GET `cells/*/own.json` 1550 → 553 at the same
  throughput.
- `0057` — Eviction deletes the superseded per-cell L0/L1 objects
  behind the handoff snapshot (see [Storage](storage.md)). Measured on
  the 200-agent shape: stored `cells/` bytes 2.47 MB → 0.57 MB, objects
  1800 → 400.
- `0061` — Warm eviction guards HEAD instead of LIST (see
  [Storage](storage.md)): one Class-B HEAD replaces one Class-A LIST.
- `0071` — Idle bundle GC goes quiet (see
  [Fleet](fleet-durability.md)): an idle bucket node costs one
  discovery LIST and then nothing until it writes, instead of one LIST
  per 30 s tick (~2880 Class-A a day). Also fixes the bucket hot-cell
  double-write (per-cell L0s beside every bundle flush).
- `0078` — Lazy handoff for short chains: an eviction skips the
  full-database handoff snapshot when the cell's restorable chain was
  born in this epoch, every acked row is already per-cell, and the
  chain holds at most 16 objects (`HANDOFF_CHAIN_MAX_OBJECTS`) — a
  successor restores the short L0 chain instead, a bounded number of
  Class-B GETs replacing one Class-A PUT plus a full-database upload
  per eviction. Modelled on ClickHouse MergeTree's lazy merges; cells
  restored from an older epoch and longer chains still snapshot.
  Measured (200 agents × 6 writes, bucket, release builds): Class A
  1934 → 1734 (−10%), write p50 unchanged at 210 ms.

## Less memory and faster starts

- `0065` — The sampler tick returns glibc free pages past
  `CELLO_C_TRIM_MB` (default 8, was a 32 MiB const). Measured
  post-drain RSS 72.0 → 61.9 MiB on the agent-50 s3bench shape.
- `0066` — `CELLO_SQLITE_CACHE_KB` caps the per-connection SQLite page
  cache (default 2000, the stock ceiling: no behavior change).
- `0067` — A deployment generation projects its `/bundle` tree once
  instead of once per isolate; read-only semantics unchanged.
- `0102` — Hydration counting is O(1) (see [Storage](storage.md)): an
  8.9 GB whale's fill no longer walks the whole cut per faulted read.

## Measure it yourself

- `0058` — A reproducible S3 request-cost harness in `tools/s3bench/`:
  `proxy.py` logs one JSON line per request in front of the S3
  endpoint, `count.py` classifies the log into `METHOD x key-family`
  counts with an R2 Class A/B summary, `stored.py` inventories stored
  `cells/` objects and bytes, `app/` is a minimal Worker + SQLite
  Durable Object app, and `run.sh` drives the agent workload (or one
  hot cell with `HOT=1`) against a local RustFS container. New files
  only; nothing under `tools/` runs in CI.
- `0069` — The s3bench agent workload drives its agents concurrently —
  one thread and connection per agent on its own phase — so arrivals
  look like independent clients and cross-cell group commit can batch.
  The serial walk kept at most one cell in flight and could never batch.
- `0072` — The s3bench drivers report per-turn latency (`mean`, `p50`,
  `p95`, `p99`, nearest-rank, whole ms) next to the request counts, so
  ack-latency regressions show beside the savings.
- `0076` — `run.sh` defaults `CELLO_BIN` to `target/release/cello` and
  refuses to start while another `cello-s3bench-*` container is up
  (unless `S3BENCH_ALLOW_PARALLEL=1`), after debug-vs-release and
  dueling-harness skews polluted two `.11` measurements.
