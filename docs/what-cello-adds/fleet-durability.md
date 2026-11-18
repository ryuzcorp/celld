---
title: Fleet and durability
description: Ownership, replication, takeover, staged activation, and bucket-posture group commit over upstream celld.
---

Patches to the fleet core: lease ownership and takeover, staged
deployment activation, request invocation against staged versions, and
the opt-in bucket-posture group commit. Includes the ported
coordination-core fixes (`0025`–`0035`). Each entry names its fork patch
number. The guarantees themselves are unchanged; see
[What Cello guarantees](../guarantees.md).

## Staged activation and invocation

- `0039` — Staged activation: `cello deploy --no-activate` uploads a
  version without moving any pointer; `cello activate SCRIPT=VERSION…`
  serves a multi-script generation with one atomic fleet-pointer write
  (service bindings resolve from the pointer's component map, queue
  attachments publish at activate time); `cello discard` removes
  versions no pointer names. See [Invoke a staged
  version](../staged-invoke.md).
- `0048` — `cello invoke` sends one HTTP request to a staged script
  version on one node, served by that version's code with its declared
  bindings against the real data — so a migration endpoint runs before
  the activation that would serve it. `POST /invoke` on the internal
  listener requires the operator token plus the fleet HMAC; a shadow
  generation serves staged pins overlaid on the live map in fresh pools
  and retires after the request. Nothing serving moves. Needs every
  node on this release (older nodes have no `/invoke` route).

## Bucket-posture group commit (opt-in)

Writes in bucket posture can share one bundle object per flush round
instead of one upload per write. It is **off by default**: an unset
`CELLO_BUCKET_BUNDLES` disables grouping and only an explicit `1`
enables it (`0` still disables; anything else stops startup).
`CELLO_BUCKET_BUNDLE_LINGER_MS` (default 0, cap 1000) holds a round
open to batch more, but only while more than one cell is dirty — a lone
cell never waits.

- `0059` — Layer 1: the sans-I/O core. `BUCKET_FORMAT` becomes 3; a node
  writes shared bundle proofs only while every live lease reads format
  3, judged per feature against its own minimum so the bump never
  closes paged epochs on a mixed fleet.
- `0060` — Layer 2: the executor. A bucket node with an open gate
  publishes its bucket-session record before the first bundle-credited
  ack and credits through the same watermarks as per-cell uploads.
  Event-driven flush, capped rounds, fleet path untouched, bucket
  posture only (`CELLO_DURABILITY=bucket`).
- `0061` — The eviction guard proves a journaled epoch with a HEAD on
  the newest journaled key instead of a prefix LIST (one Class-B HEAD
  replaces one Class-A LIST per warm guard).
- `0062` — Layer 1 follow-up: the per-round fence credit predicate plus
  the output-gate settlement it feeds, proven by a two-arm check
  (session record after the PUT, per-cell fence read after the proof)
  with proof-then-read ordering.
- `0063` — Layer 3: the successor fold. A bucket session's acked rows
  are restorable after death, restart, or quiet handoff; a live donor
  session's retained uncovered bundle rows fold before the restore.
- `0064` — Layer 4: drain, GC, and graceful close for bucket sessions.
  Eviction writes one per-cell snapshot from the complete local DB;
  `gc_bundles` deletes only fully-covered bundles with single-key
  `DeleteObject`s; graceful close seals only when every acked row is
  per-cell.
- `0070` / `0071` / `0075` — Rollout, the hot-cell double-write fix,
  and the final default-off. `0071` also quiets idle bundle GC: once a
  full-prefix scan leaves zero bundles and no bundle PUT was attempted
  since, GC skips its LIST until the next write — an idle bucket node
  costs one discovery LIST and then nothing.

Measured at `DELAY_MS=100`, release builds, 1 node: the 200-agent
workload drops from 1934 to 1140 Class-A requests with grouping on, but
write p50 rises from 210 to 445 ms; a hot cell with 16 writers rises
from 235 to 432 ms p50 with ~45% fewer turns. Enable
`CELLO_BUCKET_BUNDLES=1` for request-cost-sensitive workloads that
tolerate about one extra round trip per durable write. A `.10` binary
recovers a bucket a bundling `.11` wrote (whole-prefix fold, no loss
declared), so a downgrade is safe. Layers pending in `.11` shipped in
`.12`–`.13` form; do not enable production grouping until every live
lease reads format 3 (every node on the release that publishes it).

## Ported coordination-core fixes

`0025`–`0034` port correctness fixes from
[meln1k/celld](https://github.com/meln1k/celld) (Apache-2.0, same
author line kept with a `Ported-from` trailer; simulator and docs
files not ported), and `0035` follows them up:

- `0025` — A newer ownership release retires the in-flight one it
  supersedes (debug `indexed cell op N has no owning slot` panic and
  release `cell_ops` leak fixed).
- `0026` — Fence settles startup confirmation reads (debug
  `confirming cell left the owner read in Fenced` panic on the halting
  event fixed).
- `0027` — The alarm observer carries `WakeCoverage`, and wake
  maintenance reports `Confirmed` once the flusher covers the current
  installation, so a restored alarm no longer pins its cell resident
  forever.
- `0028` — A reset settles an alarm handler still running without a
  barrier as a failed handler (re-armed, op retired, wake entry kept).
- `0029` — The near-alarm eviction refusal applies only when the node
  has room to spare, so a full node sheds instead of parking a waiter.
- `0030` — A drain counts every handoff against `max_releases`, its
  pump reads the live count, wake hints and alarm timers wait out a
  balancing move, fence clears the handoff mark, and preserve settles a
  confirming read.
- `0031` — A reset drops the cell's sockets with its runtime, and a
  hibernatable socket's events wait for the restarted runtime on a
  quiescing cell (swap deadlock fixed).
- `0032` — A takeover from an expired lease that never opened a log
  waits for recovery, which CAS-seals the absent log so a stale session
  cannot open one through a renewal.
- `0033` / `0034` — 51 behavior-neutral code probes counting the
  core's rare branches, then the rule they enable: a cell whose last
  restore failed keeps its owner record — drain and balancer skip it —
  so no peer restores it from the bucket alone and loses the
  acknowledged tail.
- `0035` — Wake-coverage follow-ups: the startup alarm observation
  lands under the registry lock after registration, coverage compares
  alarm installations instead of snapshots, and a coverage-only
  confirmation no longer cancels a non-drain eviction.

## Fleet operations and takeover

- `0009` — `cello deploy` treats a lost content-tag race as success:
  when tagging the built image fails with `AlreadyExists`, the tag is
  re-inspected and the deploy continues if it names the same content.
- `0088` — A stale-target 409 proves a member is gone: only a 409
  `WrongTarget` carrying the peer-auth gate's stale-route marker counts;
  every other seal failure stays inconclusive and retries
  ([upstream #273](https://github.com/denoland/celld/issues/273)).
- `0089` — The renewal lease readback is bounded to the remaining
  authority (`authority_expires_ms`), so a stalled store cannot wedge
  every renewal until the fence timer
  ([upstream #264](https://github.com/denoland/celld/issues/264)).
- `0090` — The first alarm-deadline expiry only marks the firing; the
  running copy settles the alarm, so a slow handler is never doubled. A
  second expiry with the op still outstanding retires and re-fires,
  keeping alarms at-least-once
  ([upstream #256](https://github.com/denoland/celld/issues/256)).
- `0091` — Follower tails stream under a 10 s progress timeout with a
  5 s connect budget, and the transfer beats the recovery claim while
  streaming, so a healthy-but-slow tail holds its claim
  ([upstream #250](https://github.com/denoland/celld/issues/250)).
- `0092` — The node-session id is pinned in a `node-id` file beside the
  follower fragments (tmp, fsync, rename, dir fsync), so a restart
  reuses its id instead of stranding leases behind `WrongTarget`. An
  explicit `CELLO_NODE` wins outright and is recorded; a corrupt pin
  warns and regenerates. Set `CELLO_WATCH` to a persistent directory to
  keep the id — the PID-scoped default temp dir mints a new one every
  boot ([upstream #249](https://github.com/denoland/celld/issues/249)).
- `0093` — Transient CAS conflicts retry twice with the same guard
  before paying a readback round trip
  ([upstream #239](https://github.com/denoland/celld/issues/239)).
- `0094` — An absent session record counts as closed: the fragments are
  garbage no new epoch will adopt, and the sweep declares the loss in a
  `log/<node>.loss.json` record — bounded and loud instead of silent
  ([upstream #245](https://github.com/denoland/celld/issues/245)).
- `0095` — A degraded ensemble whose membership still matches its
  record resumes without an epoch step, up to 3 consecutive resumes
  before stepping anyway so a healed member can rejoin
  ([upstream #268](https://github.com/denoland/celld/issues/268)).
- `0096` — The periodic scan sleeps until the next upcoming alarm
  minute, not the next 60 s tick
  ([upstream #308](https://github.com/denoland/celld/issues/308)).
- `0097` — Docs only: guarantees cover the pinned node-session id,
  witness grace, the loss record, same-guard conflict retries, the
  authority-bounded readback, the mark-first alarm deadline, and
  wake-for-the-upcoming-minute timing
  ([upstream #244](https://github.com/denoland/celld/issues/244)).
- `0103` — Fragments of record-less sessions are collected once absence
  is terminal (no lease at all, or a superseding generation); a live
  logless lease's fragments are kept
  ([upstream #259](https://github.com/denoland/celld/issues/259)).
- `0105` — After an unclean restart the boot prunes live-named epochs
  (only a clean reload resumes one) and replays the take-once boot
  list for ownership confirmation
  ([upstream #290](https://github.com/denoland/celld/issues/290)).
- `0126` — Transient bucket write conflicts (`OperationAborted`) retry
  with backoff instead of surfacing a lost-race error; the match reads
  the tagged service code, never the object key
  ([upstream #283](https://github.com/denoland/celld/issues/283)).
- `0129` — Dead-owner takeovers route with fleet capacity: a
  zero-capacity node hands the cell to a peer instead of piling on
  ([upstream #263](https://github.com/denoland/celld/issues/263)).
