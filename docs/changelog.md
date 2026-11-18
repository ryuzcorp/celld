---
title: Changelog
description: What shipped in each Cello release, from v0.6.3-ryuzcorp.4 to v0.1.0.
---

## v0.1.0

First Cello release. The renamed product: repo, crate, binary, and CLI
are `cello`; `CELLD_*` is `CELLO_*`; `.celld/dev` is `.cello/dev`; the
health path, telemetry names, and prose say Cello. Bucket objects, cell
SQLite, stored values, and the node-to-node wire stay `celld`-spelled
(see [Compatibility with celld](compatibility.md)). Includes everything
below; no behavior change beyond the rename.

## v0.6.3-ryuzcorp.13

Adds `0079`–`0136`. No bucket format bump and no wire protocol change
between nodes, so `.12` and `.13` nodes can share a bucket; the only new
bucket-side artifact is the `log/<node>.loss.json` loss record, which
older nodes read past, and a downgrade restores everything `.13`
wrote. Default behavior changes: the node-session id is pinned in the
data dir (`0092`), `service.version` carries the operator's application
version instead of Cello's build (`0132`), and WebSocket close codes
1001–1014 (other than the reserved no-status codes) now close instead
of throwing (`0113`).

New CLI: `cello versions [SCRIPT]` lists deployed versions (`0123`);
`cello d1 --local` runs execute, export, and migrations against the
running `cello dev` (`0121`); `cello dev --state DIR` (or
`CELLO_DEV_STATE`) keeps local state outside the project (`0124`).

- Runtime and cancellation: `0079`, `0080`, `0081`, `0083`, `0084`,
  `0085`, `0086`, `0087`, `0118` settle shared, waiter-owned, and
  background work loudly instead of hanging; `CELLO_MAX_CELLS_PER_ISOLATE`
  caps cells per isolate (default 32) (`0135`);
  `CELLO_BACKGROUND_BUDGET_S` ends post-response background work past
  the budget (default 30 s) without failing the answered request
  (`0101`); a poisoned cell restarts on a fresh isolate (`0104`);
  `AbortSignal.timeout` frees its slot (`0106`); the budget-orphaned
  workflow drive drops on retry (`0125`); failed covered alarms disarm
  while uncovered ones re-arm at scan pace (`0127`).
- Fleet and durability: `0088`–`0096` bound seal proof, renewal
  readbacks, alarm deadlines, tail streaming, lease conflicts, loss
  declaration, ensemble resumes, and wake timing; `0103` collects
  record-less fragments; `0105` prunes live epochs after unclean
  restarts; `0126` retries transient bucket write conflicts; `0129`
  routes dead-owner takeovers with capacity.
- Storage: `0098`, `0105`, `0099`, `0122`, `0100` cover compaction
  retention, unclean-restart pruning, the single-writer dev store,
  indexed local LISTs, and the repaired facet-image import;
  `CELLO_SQL_STATEMENT_MS` bounds per-statement time (unset means
  unbounded) (`0128`); hydration counting is O(1) (`0102`).
- Workers compatibility: `0109`–`0117`, `0108`, `0107`, `0130`, `0113`;
  `0119` refuses foreign-`script_name` bindings at deploy time; `0131`
  reserves the host-injected `CELLO_NODE` name.
- Telemetry: `0132`–`0134`, `0136`.
- New env knobs and their defaults: `CELLO_BACKGROUND_BUDGET_S` (30),
  `CELLO_MAX_CELLS_PER_ISOLATE` (32), `CELLO_SQL_STATEMENT_MS` (unset),
  `CELLO_TMP_MAX_BYTES` (67108864, 64 MiB), `CELLO_OTEL_LOGS` (unset),
  `OTEL_RESOURCE_ATTRIBUTES` (unset), `CELLO_DEV_STATE`
  (`PROJECT/.cello/dev`), `CELLO_NODE` (generated and pinned to
  `node-id` in the data dir).

## v0.6.3-ryuzcorp.12

Adds `0076`–`0078`. No API, CLI, or bucket format change, so `.11` and
`.12` nodes share a bucket and a downgrade restores everything `.12`
wrote.

- An eviction skips the full-database handoff snapshot when the cell's
  restorable chain was born in this epoch, every acked row is already
  per-cell, and the chain holds at most 16 objects (`0078`, modelled on
  ClickHouse MergeTree's lazy merges). Measured on 200 agents × 6
  writes, bucket posture, release builds: Class A 1934 → 1734 (−10%),
  PUT 1400 → 1200, write p50 unchanged at 210 ms. On 50 agents: Class A
  582 → 532. Restored-from-older-epoch cells, group-commit rows not yet
  per-cell, and longer chains still snapshot as before.
- `tools/s3bench` defaults to the release binary and refuses to start
  while another run is up (`0076`).
- Test nodes boot on ephemeral ports, removing a port-reuse flake
  (`0077`).

## v0.6.3-ryuzcorp.11

Adds `0058`–`0067`, `0069`–`0072`, and `0075` (`0068`, `0073`, `0074`
parked unmerged). No API or CLI change. A `.11` node publishes bucket
format 3, which `.10` nodes read, so `.10` and `.11` nodes share a
bucket.

- Bucket-posture group commit (`0059`–`0064`, `0070`, `0071`) is in the
  binary but off by default (`0075`); `CELLO_BUCKET_BUNDLES=1` turns it
  on. It needs every live lease to read format 3 (every node on `.11`)
  and falls back to per-cell uploads at once otherwise. Measured on 200
  agents × 6 writes: Class A 1934 → 1140 with grouping on, but write-ack
  p50 210 → 445 ms; on a hot cell, p50 235 → 432 ms. Enable it only
  where request cost matters more than write latency. `.10` recovers a
  bucket `.11` wrote bundles into (whole-prefix fold, no loss
  declared), so a downgrade is safe.
- An eviction proves a journaled epoch with one HEAD instead of a
  prefix LIST (`0061`).
- The C heap is trimmed sooner after evictions (`0065`,
  `CELLO_C_TRIM_MB`, default 8). Post-drain RSS fell ~10 MiB on the
  agent-50 shape.
- `CELLO_SQLITE_CACHE_KB` caps the per-connection SQLite page cache
  (`0066`; default 2000 KiB is unchanged).
- A deployment generation projects its `/bundle` tree once instead of
  once per isolate (`0067`).
- `tools/s3bench` (`0058`, `0069`, `0072`) measures bucket request
  counts and per-write latency against a local S3 for cost claims.

## v0.6.3-ryuzcorp.10

Adds `0054`–`0057`, all bucket-request and storage savings; no format
or API change, so `.9` and `.10` nodes share a bucket.

- A bucket-posture eviction makes fewer requests (`0054`). An eviction
  that publishes a handoff snapshot then deletes, in the background,
  the per-cell L0/L1 objects the snapshot supersedes — one single-key
  `DeleteObject` per key (`0057`). The bucket credential needs
  `s3:DeleteObject` on `cells/`, which the read-write state grant
  already covers; a failed delete only leaves garbage behind and never
  fails the eviction.
- Concurrent bucket-posture writes on one cell share ownership reads
  (`0056`): fewer `cells/<cell>/own.json` GETs on hot cells at the same
  acknowledgement rule.
- A fleet-posture node with no peer it could ever recruit backs its
  `nodes/` scan off to the 30 s maintenance tick (`0055`), so a node
  that starts next to it is recruited within ~30 s instead of ~1 s (the
  new node recruits the old one itself at once).

## v0.6.3-ryuzcorp.9

Adds `0052`–`0053`.

- No behavior change with `CELLO_DEPLOY_AWS_*` unset. A platform that
  sets the trio must give the deployment credential read access to both
  `deploy/` and `deploy-blobs/`, and keep the ordinary `AWS_*` grant
  read-write over the state prefixes (`cells/`, `log/`, `nodes/`,
  `fleet/`, `wake/`, `drain/`, `kv/`, `r2/`, `probe/`, `telemetry/`).
  The runtime also GETs `wake-format.json` and LISTs `wake-v2/` and
  `wake-retired-v2/` (one read-only prefix grant over `wake-` covers
  it), and lists and deletes the `node-cells/` markers (that prefix
  stays read-write). The deploy command keeps the ordinary credential
  with write access to the deployment prefixes. A control-plane node,
  managed (`CELLO_CLOUD`) or BYO bucket alike, must not set the trio:
  it publishes `deploy/` itself, so it refuses the trio at boot.
- A serving process is non-dumpable: no core dump comes out of a node,
  and a non-root debugger or profiler cannot attach (`ptrace`
  refused); root tools are unaffected. Anything reading the node's
  `/proc/<pid>/environ`, `mem`, or `maps` as the fleet uid now gets
  `EACCES` (`0053`). The window between `execve` and `main` setting the
  flag is still dumpable, and spawned children start with an emptied
  environment (`PATH`, `HOME`, `TMPDIR` only), so no credential reaches
  them.

## v0.6.3-ryuzcorp.8

Adds `0051`: `cello deploy --read-root DIR` (repeatable) names
directories beyond the project that esbuild's inputs and
`containers[].image` Dockerfiles with their build contexts may come
from. A platform that deploys a built subdirectory of a checkout, or a
monorepo app whose packages are hoisted above its config, names the
checkout. Without the flag, `.7`'s project-only bounds are unchanged.

## v0.6.3-ryuzcorp.7

Adds `0043`–`0050`.

- `cello deploy` and `cello d1` refuse a project path that resolves
  outside the project directory — the config file's own directory
  (`0050`). That covers the config, `main`, assets, Dockerfiles and
  build contexts, `migrations_dir`, Python sources, and every file
  esbuild bundles. A monorepo that hoists `node_modules` above that
  directory no longer bundles: install the packages inside it.
  `migrations_dir` entries must be regular files, and a
  `containers[].image` that is an absolute or `..` path is refused.
- On Linux, esbuild runs under Landlock with reads confined to the
  project, so `CELLO_ESBUILD` (or `esbuild` on `PATH`) must be the
  self-contained release binary, not a wrapper script (`0050`). A
  kernel or seccomp profile without Landlock runs it unconfined with a
  warning; Docker's, containerd's, and podman's default profiles allow
  it.
- `cello r2 head` and `cello r2 get --json` print a
  `{"error":"not_found",…}` object on stdout for an absent key (`0044`);
  exit code and stderr unchanged.
- `cello --version` reports the full build version (`0046`).
- New, opt-in: `CELLO_CACHE_SCOPE=deployment` (`0045`); `cello invoke`
  runs one request against a staged version before `cello activate`
  (`0048`), with `--header-env` for secret headers (`0049`);
  `cello deploy --sourcemap` collects a `no_bundle` entry's sibling map
  (`0047`).
- `cello invoke` needs every node on this release. A stream received
  over RPC now stays open while the receiver holds it (`0043`), ending
  the spurious aborts of signal-carrying container execs.

## v0.6.3-ryuzcorp.6

Adds `0009`–`0042`.

- Upgrade every node of a fleet together. A rolling upgrade moves cells
  with no migration (`0037`), but cross-version traffic fails until
  every node runs this release: operator calls through an old node fail
  closed on new nodes, Queue DLQ and redrive forwards fail in both
  directions, and stubs sent from an old node revive as plain data on a
  new one (`0040`, `0041`). The Queue alarm replays the durable DLQ and
  redrive marks once the fleet converges.
- Storage rows holding stubs written before the upgrade read back as
  plain data (Durable Object stubs as null). Get each stub again from
  its namespace or binding and store it again to write the new envelope
  (`0040`); storing the read-back value does not restore a stub.
- Workers now fail when they reach a resource their script never
  bound, touch another cell's storage, call D1, KV, or Queue operator
  methods through stubs, or use a socket id outside its owning cell or
  request (`0022`, `0040`, `0041`, `0042`). Declare every binding a
  Worker uses, and run operator actions through the `cello` CLIs.
- The Workers Cache API takes up to 64 MiB of memory per node by
  default; `CELLO_CACHE_MAX_BYTES=0` turns it off (`0018`).
- Shared D1, KV, and Queue cells move to a runtime pool that `/state`
  reports as `__celld-runtime` (`0037`).
- Opt-in hardening stays off by default: `CELLO_EGRESS` is `open`
  (`0021`), `CELLO_FENCE_NODE_NETNS` is `off` (`0014`),
  `CELLO_MEMORY_SCOPE` is `container` (`0011`), and
  `CELLO_OPERATOR_TOKEN` is unset (`0013`).
- Set `CELLO_OPERATOR_TOKEN` only once every node runs this release:
  older nodes send no token, so a mixed fleet with the token set fails
  internal calls with 401 (`0013`). Set it in the environment of every
  `cello` CLI too; liveness and readiness probes use the public health
  endpoint.
- Queue rows whose dead-letter source was forged before the upgrade can
  still be redriven to the name they carry; planting such rows is
  closed by `0041`, redrive is operator-only, and retention ages them
  out.

## v0.6.3-ryuzcorp.5

Adds `0007`–`0008` (durable Object RPC across isolates; container exec
`exitCode` survival).

## v0.6.3-ryuzcorp.4

Patches `0001`–`0006`: per-container image selection, exec waiting on
its start, `node:crypto` scrypt, streaming-body `Response.clone()`,
service-binding RPC stubs, and container exec pty. First release
published by the fork.
