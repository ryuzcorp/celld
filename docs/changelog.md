---
title: Changelog
description: What shipped in each Cello release, from v0.6.3-ryuzcorp.4 to v0.2.3.
---
## Unreleased

A `TransformStream` now applies backpressure, a `tee()` bounds the skew
between its branches, compression streams no longer leak native memory, and
an isolate's `ArrayBuffer` memory is capped. No data or bucket-format
changes; `CELLO_V8_EXTERNAL_LIMIT_MB` is new and optional.

- **`TransformStream` applies backpressure** (#105). Its writable side used to
  resolve `write()` as soon as the transform returned, whether or not
  anything read the readable side, so a Worker streaming a
  `TransformStream` into a response buffered the whole body in the
  isolate's heap while its HTTP client did not read -- and `near_heap_limit`
  grows the heap rather than failing, so one non-reading client could run
  the process out of memory. The writable side now waits for the readable
  side to be read, as Workerd does: `await writer.write(...)` and
  `writer.ready` stay pending until a consumer pulls, end to end through
  the response-body pump. Cancelling the readable, aborting the writable,
  `controller.error()`, and `controller.terminate()` release a write parked
  on the handshake (with the stored error, or a `TypeError` for
  `terminate`), and an error or terminate issued from inside `start()` is
  applied as soon as the writable exists, so a later write rejects instead
  of parking. A producer that writes without awaiting either,
  or that writes past `writer.desiredSize` 0, still queues in memory;
  `writer.ready` is the signal to honor.
- **Compression streams release their native state** (#106). Every
  `CompressionStream` and `DecompressionStream` held its zlib state in one
  process-wide table that only a finished, cancelled, or aborted stream left.
  An abandoned stream, a stream whose input was not bytes, or a torn-down
  isolate kept its entry forever, so one tenant constructing streams in a loop
  could leak native memory for every tenant on the node. The table is now
  per isolate and goes with it. A stream left unreachable mid-flight is
  released when it is garbage-collected, and a transform error releases it
  too. An isolate holds at most 128 live compression streams; the next
  constructor call throws a `RangeError`.
- **A `tee()` bounds the skew between its branches** (#107). A Worker that
  read one branch of `response.body.tee()` -- or of a `Response.clone()` or
  `Request.clone()` of a streaming body it constructed -- to the end while
  the other branch went unread buffered the whole body in the isolate's
  heap, and `near_heap_limit` grows the heap rather than failing, so one
  tenant could run the shared process out of memory. The tee now charges
  each chunk to both branches; the difference between what they hold is
  measured just after a chunk is queued, and past 32 MiB -- the host tee's
  budget for platform bodies -- the branch that is behind fails its next
  read with `tee branch fell more than 32 MiB behind its sibling; read both
  branches concurrently`, while the branch being read keeps flowing to the
  end with the source uncancelled. The chunk size alone fails no branch,
  but a branch that has not read an oversize chunk yet holds all of it and
  fails for lag, so with chunks larger than the budget both branches must
  be read concurrently. A chunk is charged its size (2 bytes per UTF-16
  code unit for a string, its length for a buffer, its visible length for a
  view) and at least 64 bytes. That makes the budget a guard against
  accidental buffering rather than an isolate memory limit: a payload
  hidden in an object chunk is not counted, and the budget is per `tee()`,
  so a chain of N clones can hold N × 32 MiB.
- **A tee of a tee keeps its read path under the shared budget** (#111). The
  32 MiB budget one request's tees share picked its victim by buffered bytes
  alone, and the `HashMap` iteration order broke ties. In a chain that tie is
  the common case -- the inner `tee()`'s source and an unread copy hold
  exactly the same bytes until the inner pump runs -- and once the chain
  charges the same bytes at two stages the inner tee's source is often the
  largest holder outright, so the budget kept failing the copy its own tee
  was draining. The inner tee then treated that as its source ending and
  closed both of its copies: a Worker's `await copy.arrayBuffer()` (or a
  subrequest streaming a copy) got an empty or truncated body under load and
  no error at all, while the other copies carried on. The budget now ends a
  copy nobody has read from -- the one holding the most, when it gives back
  enough on its own -- and fails a copy a consumer is draining only when no
  unread copy does. A copy is spared once its consumer has taken a byte, and
  a copy an inner tee is draining is spared from the moment it is teed;
  before its first byte a copy a subrequest is streaming is
  indistinguishable from an unread copy, so a fetch streaming one can itself
  be the victim. Either way the failed copy rejects with `tee branch fell
  more than 32 MiB behind its sibling; read both branches concurrently` --
  including a read already in flight, and a send that a source termination
  races -- instead of ending the body as if it were complete, so a Worker
  never receives a truncated body without an error. The other copies keep
  flowing.
- **A failed `fetch()` reports its root cause** (#111). The error message used
  to be reqwest's wrapper (`error sending request for url (…)`); it now
  names the cause, such as `tcp connect error: Connection refused`,
  `operation timed out`, or the tee skew message when a streamed request body
  was a tee copy the budget ended. Code that matched the old wrapper text
  should match the cause instead.

- **An isolate's `ArrayBuffer` memory is capped** (#110). V8's default
  ArrayBuffer allocator is `malloc`, so `new ArrayBuffer(n)` in a Worker was
  an unbounded allocation inside the shared process: `near_heap_limit`
  extends the V8 heap limit and never fails the allocation, and backing
  stores are external memory rather than heap. One tenant looping
  `new Uint8Array(8 << 20)` grew the node by 640 MiB with a 128 MiB heap
  limit and was never refused (measured: RSS +643 MiB, isolate not
  condemned). Every isolate now has an allocator of its own that charges
  each backing store to a per-isolate budget and returns null past it, which
  V8 turns into a catchable `RangeError: Array buffer allocation failed`;
  the refusal condemns the isolate, which retires it so the cell restarts on
  a fresh one. The budget is `CELLO_V8_EXTERNAL_LIMIT_MB`, four heap limits
  by default (512 MiB), above the 64 MiB `/tmp` read, 32 MiB WebSocket
  message, 32 MiB tee skew and clone that one request legitimately holds. A
  collection returns freed buffers to the budget, so churn past the limit is
  never refused. Buffers the host hands to JavaScript -- response bodies,
  WebSocket messages, RPC arguments, `/tmp` reads, D1 blobs -- are charged
  to the same budget and refused the same way. Two paths stay outside it
  and are documented in [Limitations](reference/limits.md): a resized
  resizable `ArrayBuffer` is retired at the next turn, and
  `WebAssembly.Memory` is bounded only by V8 and the node's memory pressure.

## v0.2.3

Streamed responses no longer die after 30 seconds. No configuration,
data, or bucket-format changes.

- **A streamed response body outlives the background budget** (#103).
  Since v0.1.0, `CELLO_BACKGROUND_BUDGET_S` (30 s) also ended the response
  body pump, so an SSE stream or any long download went silent about 30 s
  after its handler answered, with the socket left open. A body now streams
  for as long as the client stays connected and the body keeps producing.
  `ctx.waitUntil()` work is still ended at the budget, also while its
  entry streams.
- **A stalled stream ends after 60 seconds without progress.** That covers
  a body that produces nothing and a client that stops reading; the
  producer is cancelled through its request signal. Send a keepalive (an
  SSE comment) at least once a minute. Workerd keeps an idle body open.
- **A client disconnect no longer waits on the body's `cancel()`.** The
  host stream closes first, so a `cancel()` that never settles cannot pin
  the request.

## v0.2.2

Stream and WebSocket fixes, faster CI, and reorganized docs. No
configuration, data, or bucket-format changes.

- **A teed body no longer deadlocks** (#93). A Worker that read one
  `tee()` branch of a platform-streamed body (a `fetch()`, RPC, Durable
  Object, or R2 body) to the end before the other used to hang once 16
  chunks piled up on the unread side. The copies now buffer for each other
  up to 32 MiB of skew, shared by all of one request's tees. Past that, the
  lagging copy's next read rejects with `tee branch fell more than 32 MiB
  behind its sibling; read both branches concurrently`, and the node's
  memory stays bounded.
- **WebSocket closes report what happened** (#90). A peer close that
  arrives while a send is cut short reports `wasClean: false`. A backlog
  that tripped the 16 MiB budget wins over a racing close and ends with
  1011.
- **Faster CI** (#91, #94). Lint and the full suite run natively on amd64
  and arm64 under the same hostile env as before, in about 12 minutes
  instead of about 20. Docs-only pull requests skip the Rust jobs. A
  release tag publishes only after the `main` CI run on its commit has
  succeeded, so the publish job no longer re-runs the tests. The flaky
  `shutdown_during_parked_send_ends_pump` test is now deterministic.
- **Docs** (#92, #95–#100). The site is reorganized into Guides,
  Services, Concepts, and Reference, with a new quickstart, configuration
  and CLI references, and one page per service. Old URLs redirect.
- **Test harness** (#89). `CELLO_ESBUILD` reaches tests that run under
  the hermetic env.

## v0.2.1

WebSocket robustness. No configuration, data, or bucket-format changes.

- **Backpressure-safe WebSocket pipes** (#83, #87). The upgrade pump and
  the cell pipes no longer block on a full queue. A peer that stops reading
  is held to a 16 MiB backlog per direction, and the socket then ends
  loudly with 1011. A single message of up to 32 MiB still passes, whatever
  smaller backlog is ahead of it. Closes stay observable while a send is
  parked, and teardown is bounded. A slow but live peer or a slow handler
  is never timed out.
- **WebSocket messages are capped at 32 MiB** (#85), the Cloudflare
  Workers limit, whether sent as one frame or as fragments. A peer that
  streams continuation frames past the cap gets close code 1009 instead
  of growing the node's memory without bound, and the cell sees
  `(1009, reason, false)`. An over-cap frame is refused from its header.
  The reader holds under about 100 MiB per socket while messages are read;
  `docs/cloudflare-compat.md` states the full per-direction bounds.

## v0.2.0

Simple backward compatibility with
[denoland/celld](https://github.com/denoland/celld): swap the binary or
image on the same bucket. Stop every old node first — the peer protocol
is new (`cello-tunnel`, `x-cello-overload`, `cello-log-stream`, new
overload marker), so a mixed fleet is unsupported — then start 0.2.0.
Rename every `CELLD_*` variable to `CELLO_*`; the node refuses to start
otherwise, fail-closed.

Persisted identifiers are now written with the `cello` spelling and read
with both: `__cello-runtime`, the `__cello$*` stub envelopes (rows
written before the binding-isolation patches still decode as plain
data), `cello-r2` metadata and `cello-r2-cursor-*` cursors,
`cello-schema` / `cello-retention`, and the `cello-image:` prefix. Data
an older node wrote carries over with no migration step and no rewrite
pass. In-process dunders and the wire identifiers were renamed outright.
See [Compatibility](guides/upgrade.md).


## v0.1.0

First Cello release. The renamed product: repo, crate, binary, and CLI
are `cello`; `CELLD_*` is `CELLO_*`; `.celld/dev` is `.cello/dev`; the
health path, telemetry names, and prose say Cello. Bucket objects, cell
SQLite, stored values, and the node-to-node wire stay `celld`-spelled
(see [Compatibility with celld](guides/upgrade.md)). Includes everything
below; no behavior change beyond the rename.

Releases `v0.6.3-ryuzcorp.4` through `.13` shipped as celld, so their
entries below use the historical spellings they shipped with —
`CELLD_*`, `.celld/dev`, and the `celld` binary — not the current
`CELLO_*` names.

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

New CLI: `celld versions [SCRIPT]` lists deployed versions (`0123`);
`celld d1 --local` runs execute, export, and migrations against the
running `celld dev` (`0121`); `celld dev --state DIR` (or
`CELLD_DEV_STATE`) keeps local state outside the project (`0124`).

- Runtime and cancellation: `0079`, `0080`, `0081`, `0083`, `0084`,
  `0085`, `0086`, `0087`, `0118` settle shared, waiter-owned, and
  background work loudly instead of hanging; `CELLD_MAX_CELLS_PER_ISOLATE`
  caps cells per isolate (default 32) (`0135`);
  `CELLD_BACKGROUND_BUDGET_S` ends post-response background work past
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
  `CELLD_SQL_STATEMENT_MS` bounds per-statement time (unset means
  unbounded) (`0128`); hydration counting is O(1) (`0102`).
- Workers compatibility: `0109`–`0117`, `0108`, `0107`, `0130`, `0113`;
  `0119` refuses foreign-`script_name` bindings at deploy time; `0131`
  reserves the host-injected `CELLD_NODE` name.
- Telemetry: `0132`–`0134`, `0136`.
- New env knobs and their defaults: `CELLD_BACKGROUND_BUDGET_S` (30),
  `CELLD_MAX_CELLS_PER_ISOLATE` (32), `CELLD_SQL_STATEMENT_MS` (unset),
  `CELLD_TMP_MAX_BYTES` (67108864, 64 MiB), `CELLD_OTEL_LOGS` (unset),
  `OTEL_RESOURCE_ATTRIBUTES` (unset), `CELLD_DEV_STATE`
  (`PROJECT/.celld/dev`), `CELLD_NODE` (generated and pinned to
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
  binary but off by default (`0075`); `CELLD_BUCKET_BUNDLES=1` turns it
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
  `CELLD_C_TRIM_MB`, default 8). Post-drain RSS fell ~10 MiB on the
  agent-50 shape.
- `CELLD_SQLITE_CACHE_KB` caps the per-connection SQLite page cache
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

- No behavior change with `CELLD_DEPLOY_AWS_*` unset. A platform that
  sets the trio must give the deployment credential read access to both
  `deploy/` and `deploy-blobs/`, and keep the ordinary `AWS_*` grant
  read-write over the state prefixes (`cells/`, `log/`, `nodes/`,
  `fleet/`, `wake/`, `drain/`, `kv/`, `r2/`, `probe/`, `telemetry/`).
  The runtime also GETs `wake-format.json` and LISTs `wake-v2/` and
  `wake-retired-v2/` (one read-only prefix grant over `wake-` covers
  it), and lists and deletes the `node-cells/` markers (that prefix
  stays read-write). The deploy command keeps the ordinary credential
  with write access to the deployment prefixes. A control-plane node,
  managed (`CELLD_CLOUD`) or BYO bucket alike, must not set the trio:
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

Adds `0051`: `celld deploy --read-root DIR` (repeatable) names
directories beyond the project that esbuild's inputs and
`containers[].image` Dockerfiles with their build contexts may come
from. A platform that deploys a built subdirectory of a checkout, or a
monorepo app whose packages are hoisted above its config, names the
checkout. Without the flag, `.7`'s project-only bounds are unchanged.

## v0.6.3-ryuzcorp.7

Adds `0043`–`0050`.

- `celld deploy` and `celld d1` refuse a project path that resolves
  outside the project directory — the config file's own directory
  (`0050`). That covers the config, `main`, assets, Dockerfiles and
  build contexts, `migrations_dir`, Python sources, and every file
  esbuild bundles. A monorepo that hoists `node_modules` above that
  directory no longer bundles: install the packages inside it.
  `migrations_dir` entries must be regular files, and a
  `containers[].image` that is an absolute or `..` path is refused.
- On Linux, esbuild runs under Landlock with reads confined to the
  project, so `CELLD_ESBUILD` (or `esbuild` on `PATH`) must be the
  self-contained release binary, not a wrapper script (`0050`). A
  kernel or seccomp profile without Landlock runs it unconfined with a
  warning; Docker's, containerd's, and podman's default profiles allow
  it.
- `celld r2 head` and `celld r2 get --json` print a
  `{"error":"not_found",…}` object on stdout for an absent key (`0044`);
  exit code and stderr unchanged.
- `celld --version` reports the full build version (`0046`).
- New, opt-in: `CELLD_CACHE_SCOPE=deployment` (`0045`); `celld invoke`
  runs one request against a staged version before `celld activate`
  (`0048`), with `--header-env` for secret headers (`0049`);
  `celld deploy --sourcemap` collects a `no_bundle` entry's sibling map
  (`0047`).
- `celld invoke` needs every node on this release. A stream received
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
  default; `CELLD_CACHE_MAX_BYTES=0` turns it off (`0018`).
- Shared D1, KV, and Queue cells move to a runtime pool that `/state`
  reports as `__celld-runtime` (`0037`).
- Opt-in hardening stays off by default: `CELLD_EGRESS` is `open`
  (`0021`), `CELLD_FENCE_NODE_NETNS` is `off` (`0014`),
  `CELLD_MEMORY_SCOPE` is `container` (`0011`), and
  `CELLD_OPERATOR_TOKEN` is unset (`0013`).
- Set `CELLD_OPERATOR_TOKEN` only once every node runs this release:
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
