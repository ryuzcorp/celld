---
title: Storage
description: Eviction savings, dev-store throughput, repair, and SQL bounds over upstream celld.
---

Patches to what Cello stores and how it reads it back: cheaper
evictions, a single-writer dev store, compaction and crash repair, and
bounds on SQL and page cache. Request-count savings live on the
[Efficiency](efficiency.md) page; each entry names its fork patch
number.

## Bucket-posture writes and eviction

- `0054` — A bucket-posture eviction skips visibility LISTs when this
  residency itself uploaded into the prefix: a successor restores by
  listing, so the uploader's own success response is proof enough. A
  resumed local copy, recovered tails, bundles, and the L0-chain
  fallback still list.
- `0056` — Concurrent bucket-posture writes on one cell share ownership
  reads: a gated write joins a read in flight on its cell and epoch
  when the issuing gate's cover reaches its position, and issues its
  own read at once otherwise. No write waits longer than before; fleet
  proofs never share reads.
- `0057` — An eviction deletes the per-cell L0 and L1 objects its
  handoff snapshot supersedes: after the snapshot PUT succeeds, a
  background task deletes this epoch's journaled upload keys (max txid
  at or below the snapshot's) with one single-key `DeleteObject` per
  key — never bulk `POST ?delete` — at bounded concurrency. The
  eviction returns at once; failed DELETEs leave restore-invisible
  garbage for epoch GC. Journal capped at 8192 entries per cell epoch.
- `0061` — Follow-up: the eviction guard proves a journaled epoch with
  one HEAD on the newest journaled key instead of a prefix LIST, falling
  back to the LIST on a miss, an error, or an empty journal.

## Local store and crash behavior

- `0098` — Compaction retains live-named epoch files: after an L1 fold
  the node keeps the newest covered L0 file that `pos()` and verify
  still read back, and each activation plus each startup drops live
  epoch dirs a newer local epoch supersedes
  ([upstream #311](https://github.com/denoland/celld/issues/311)).
- `0099` — The dev store funnels every PUT through one writer thread,
  one connection, and one queued batch per transaction. Concurrent PUTs
  no longer lose the SQLite lock lottery into `busy_timeout` sleeps: an
  idle batch costs one commit like today's single PUT, a loaded batch
  shares one fsync, and each row keeps its own savepoint so
  create/update conflicts still fail alone
  ([upstream #310](https://github.com/denoland/celld/issues/310)).
  Local dev-store only; fleet and bucket durability paths untouched.
- `0100` — The 0.5.1 facet-image import is repaired: the check is
  no-user-data instead of `page_count == 0`, which never fired under
  replication, and the decision is stamped in `user_version` so a later
  wipe never resurrects it
  ([upstream #292](https://github.com/denoland/celld/issues/292)).
- `0105` — Unclean restarts prune live-named epochs (see
  [Fleet](fleet-durability.md)).
- `0122` — Local LISTs read from a covering index over a prefix range
  instead of scanning the whole `objects.sqlite3`: the open creates
  `objects_listing` and bounds the scan to the prefix, with the
  Rust-side prefix check as backstop. No new knob; dev-store only
  ([upstream #265](https://github.com/denoland/celld/issues/265)).

## Bounds and budgets

- `0066` — A per-connection SQLite page-cache budget: `finish_open`
  sets `PRAGMA cache_size=-<kb>` from `CELLO_SQLITE_CACHE_KB`
  (default 2000, the stock ceiling: no behavior change at merge),
  validated at startup.
- `0102` — Hydration counting is O(1): the hydration set carries an
  exact cut-relative count maintained on every insert and truncation,
  so a faulted read no longer walks the whole cut
  ([upstream #270](https://github.com/denoland/celld/issues/270)).
- `0128` — Per-statement SQL time is bounded by
  `CELLO_SQL_STATEMENT_MS`: when set, each statement is interrupted
  past the bound and fails with a catchable timeout error that rolls
  the statement back; unset means unbounded, the historical behavior.
  Streaming cursors share their call's single deadline, and a timeout
  flag from an armed run never rewrites a later unarmed error
  ([upstream #274](https://github.com/denoland/celld/issues/274)).
