# ryuzcorp/celld

This is ryuzcorp's fork of [denoland/celld](https://github.com/denoland/celld).
Upstream ships only security fixes from 0.6.x on, so this fork carries the
feature patches we need on top of the pinned upstream releases. Upstream's
README below is kept intact; this file documents the fork.

## Branches

- `main` mirrors upstream `main` and carries no patches.
- `ryuzcorp/v<version>` is a line branch: the upstream tag `v<version>`
  plus our patches as commits, one commit per patch, in `0001`-`0049`
  order. Each patch commit is self-contained and carries its own tests.

Upstream's `.github/workflows/release.yml` targets `ghcr.io/denoland` and
an upstream candidate branch, so the line branches replace it with our
own `ci.yml` (build and test both architectures, push nothing) and
`publish.yml` (publish release tags); `main` keeps upstream's file
(see "Upstream release pipeline" below).

Line branches accept changes only through pull requests: direct
pushes, force-pushes, and deletions are blocked by ruleset (see
"Rulesets").

## Tags and images

An annotated tag `v<version>-ryuzcorp.<n>` on the tip of the matching
line branch is a release: `publish.yml` verifies the tag names a
`<version>`/`<n>` pair, the tree's `Cargo.toml` agrees on `<version>`,
the upstream `v<version>` tag exists on origin and is an ancestor of
the tagged commit, and the tagged commit is the current tip of
`ryuzcorp/v<version>`; it refuses to overwrite an already-published
level, and publishes the multi-arch image

    ghcr.io/ryuzcorp/celld:<version>-ryuzcorp.<n>

with labels `org.opencontainers.image.source`,
`org.opencontainers.image.version`, `org.opencontainers.image.revision`
(the tagged commit), and `now.ryuzcorp.celld.upstream` (the upstream
tag's commit). Each architecture build records SLSA provenance and an
SBOM, and the multi-arch index carries a provenance attestation.
Consumers pin the image by digest and can verify the attestation:

    gh attestation verify oci://ghcr.io/ryuzcorp/celld@<digest> --repo ryuzcorp/celld

Images published before the `-ryuzcorp` rename carry the legacy tag
names `0.6.3-noite.1..3`; they are the same commits as `ryuzcorp.1..3`.

## Adding a patch

Open a pull request into the line branch: one commit, self-contained,
with tests. Nothing lands without passing the merge gate:

- the required `ci.yml` checks are green: `Test and build (amd64)`
  and `Test and build (arm64)`;
- an independent security review of the PR is recorded, with every
  High and Medium finding fixed and re-verified before merge.

This fork is agent-run: there is no human backstop. The security
review must explicitly diff `.github/` and `Dockerfile` on every PR —
a PR runs its own `ci.yml`, so a patch that neuters its own tests
still shows green checks (accepted risk, see below).

## Cutting a release

After the patch PR merges, an admin tags the line tip and pushes the
tag; `publish.yml` does the rest:

    git tag -a v<version>-ryuzcorp.<n> <commit> -m '...'
    git push origin v<version>-ryuzcorp.<n>

The tag must be annotated and must point at the current tip of
`ryuzcorp/v<version>` — tags on older commits are refused — and
`<version>` must match the tree's `crates/celld/Cargo.toml`. Tag-push
access is publish access. A published level is never rebuilt, so cut
a new `<n>` for every change, and never reuse an `<n>`.

## Moving to a new upstream release

Mirror the new upstream tag to origin before starting the new line —
publish resolves `v<new>` from origin, and refuses without it:

    git fetch upstream --tags
    # confirm v<new> is the expected upstream commit
    git push origin v<new>

then create the new line branch at the bare upstream tag, and carry the
patch commits over on a separate patch branch (never rebase the old line
branch itself; it keeps serving its published tags):

    git push origin v<new>:refs/heads/ryuzcorp/v<new>
    git checkout -b patches-v<new> ryuzcorp/v<old>
    git rebase --onto v<new> v<old>

then resolve conflicts, run the full test suite, and open a pull request
from `patches-v<new>` into `ryuzcorp/v<new>`.

## Rulesets

The pipeline's checks are only as strong as the branch and tag
protection behind them. These rulesets are live:

- `patched lines` on `refs/heads/ryuzcorp/**`: pull requests only (no
  direct pushes), 0 approvals, strict required checks `Test and build
  (amd64)` and `Test and build (arm64)`, linear history with
  rebase-only merges, no force-push, no deletion, and resolved-threads
  required. Creating a new line branch is exempt from the required checks
  (it starts at a bare upstream tag); everything after goes through pull
  requests. The repo allows rebase merges only.
- `release tags` on `v*-ryuzcorp.*`: creation restricted to repo
  admins, no update, no deletion.
- `upstream tags immutable` on `v[0-9]*`: no update, no deletion (the
  mirrored upstream tags are provenance inputs).
- `main and candidate`: no deletion, no force-push.
- `no candidate branch`: creation of a branch named `candidate` is
  blocked (see below).

## Upstream release pipeline

Upstream's `.github/workflows/release.yml` survives only on `main`
(and the mirrored upstream tags); every line branch deletes it.
Never create a GitHub Release on an upstream tag in this fork:
`release: published` would run upstream's container phase — it can't
push to `ghcr.io/denoland`, but still. Pushing a branch named
`candidate` would likewise run upstream's candidate pipeline from
`main`'s copy, so that name stays blocked by ruleset.

## Accepted risks

- Self-graded CI: a patch PR runs its own `ci.yml`, so required
  checks alone cannot prove the tests ran. Control: the merge gate
  above — an independent security review on every PR that explicitly
  diffs `.github/` and `Dockerfile`, with all High/Medium findings
  fixed and re-verified. A future `pull_request_target` trusted
  builder would harden this further.
- Shared build cache: the `buildcache-amd64/arm64` refs are also
  written by Noite's `celld.yml` until the migration completes. Both
  writers are trusted pipelines and BuildKit cache objects are
  content-addressed, so this is acceptable; remove Noite's write grant
  once its workflow is retired.

## Patches

- `0001` (`e8ab11d`) `ctx.container.images` plus `start({ image })`:
  per-container image selection, with `images.default` for callers that
  read it.
- `0002` (`ef6e6f3`) `exec()` waits for the `start()` before it instead
  of answering 404 no such container.
- `0003` (`278c255`) `node:crypto` `scrypt` and `scryptSync` with Node's
  parameters, defaults, and error codes.
- `0004` (`f88f52c`) `Response.clone()` tees a streaming body like
  `Request.clone()` does, and both `clone()`s refuse a body already read
  through its reader, as Fetch does.
- `0005` (`c7a0653`) service-binding RPC carries Durable Object stubs
  both ways: a `WorkerEntrypoint` method can return or accept a stub of a
  namespace it holds, while RpcTargets, functions, and streams still fail
  with `DataCloneError`.
- `0006` (`81ebacb`) container exec `pty` (`true` or `{ cols, rows }`,
  default 80x24), `isPty`, and `resize(cols, rows)`; a failed first
  resize kills the exec instead of orphaning it.
- `0007` (`5bea20d`) Durable Object RPC exports cross isolates on the
  same node: stubs mint under host-routable ids validated on every op,
  and `ReadableStream` args (with the abort-signal carrier) cross by
  caller-side push into a bounded, backpressured host channel. Same
  node only; caller-owned `WritableStream` args keep their
  same-isolate marker; mint caps 4096/scope and 65536/process fail
  the call with a clear error.
- `0008` (`750e18a`) container exec `exitCode` survives the exec event:
  a retained wait re-subscribes in each later event instead of hanging
  on the first event's dropped op; awaiting in its own event still
  resolves through its own subscription.
- `0009` (`d4215ef`, test fix-ups `145d8e9` and `56bfc42`, the latter
  running the fake engine through `sh`) `celld deploy` treats a
  lost content-tag race as success: when tagging the built image fails
  with `AlreadyExists`, the tag is re-inspected and the deploy continues
  if it names the same content; a different image under the tag still
  fails.
- `0010` (`697378e`) container exec honors `options.signal`
  (AbortSignal): an already-aborted signal refuses with its reason
  before spawning, aborting later sends SIGKILL through the existing
  exec kill path, and the abort listener is removed when the process
  exits.
- `0011` (`4acf827`) `CELLD_MEMORY_SCOPE=self` measures the
  process's own cgroup v2 leaf instead of the container root, so nodes
  sharing one container each shed against their own `memory.max` (which
  an unset `CELLD_MAX_RSS_MB` defaults to a share of); cgroup v1 and an
  unreadable leaf fall back to the root with a warning.
- `0012` (`85d4886`) `celld deploy --sourcemap PATH` writes the
  bundled Worker's external source map to a local file: the esbuild run
  adds `--sourcemap=external`, so the deployed module bytes are
  byte-identical with or without the flag (a trailing
  `sourceMappingURL` comment is stripped defensively), and the map's
  `sources` are rebased to project-root-relative paths (outside the
  root a `../` path, unresolvable ones their file name). Only the
  bundled JS entry is mapped, never wasm siblings; asset-only,
  `no_bundle`, and Python projects warn and write nothing. The Docker
  test stage installs esbuild, pinned by sha512, so the bundling tests
  run in CI.
- `0013` (`176a0e3`) `CELLD_OPERATOR_TOKEN` guards the internal
  listener: when set, every internal route requires
  `Authorization: Bearer <token>` (constant-time compare, 401 without
  it, no exempt routes — probes use the public health endpoint) and the
  node refuses to start on an empty value or one with surrounding
  whitespace; unset is today's open listener. Every in-tree internal
  client (peer forwarding, handoff, abort, log tier, probe,
  `d1`/`kv`/`queue` CLIs, `diagnose`, `dev` shutdown) sends the token
  from its environment.
- `0014` (`043fd8b`) container fence admits replies to a node in
  a container netns: `CELLD_FENCE_NODE_NETNS=auto` (default `off`,
  byte-identical) cuts an `established,related` exception for the
  node's own addresses, discovered on Linux or pinned with
  `CELLD_FENCE_NODE_ADDRS` (a pin listing no address refuses). Replies
  only — a container still cannot open a connection to the node — and no
  per-container ownership: a hoster with untrusting tenants on one engine
  still enforces which containers a node may dial above celld.
- `0015` (`180ce26`) container exec kill reaches the process:
  the kill op runs under the start's lifetime (a fire-and-forget kill
  no longer dies with its event), and the signal is delivered by an
  in-container scan for the exec's marker env instead of a host-pid
  `kill` (the daemon reports host PIDs, and images may lack a `kill`
  binary). A killed exec's `exitCode` settles with its signal exit,
  and the container is untouched. The image needs `sh`, `grep` and
  `/proc`; an undelivered kill logs `container_exec_kill_failed`. This
  also makes `0010`'s abort signal reach the process.
- `0016` (`219b790`) container exec reads survive their
  subscribing event: a read issued in one event and consumed in
  another (a read-ahead, a terminal frame) re-subscribes per event
  like a retained wait instead of hanging on the dropped op, while
  the pipe stash hands a consumed-but-discarded chunk to the chained
  re-subscription and the lease slot turns a late pile-up aside, so
  bytes arrive exactly once. Test-only in-memory exec processes
  stand in for the daemon where no engine exists.
- `0017` (`9aae151`) `celld r2 list` takes `--delimiter` and,
  with `--page`, prints one page — decoded keys with `httpMetadata`
  and `customMetadata`, plus `delimitedPrefixes`, `truncated` and
  `cursor` — through the same listing the R2 binding calls; a truncated
  listing continues with `--cursor`, because `startAfter` compares
  decoded keys in UTF-8 byte order while the store walks the encoded
  form, so continuing by `--after` can omit keys. `--json` keeps
  upstream's one-record-per-line output.
- `0018` (`aa4d061`) per-node Workers Cache API: `caches.default` and
  `caches.open(name)` over a process-local store keyed by the script name
  the host read from isolate state, so all isolates of a script on the
  node share entries and no script can name another's. Cloudflare
  freshness (`s-maxage`/`max-age`/`Expires`, edge-TTL defaults),
  `no-store`/`private`/`Set-Cookie` never cached, `Vary` ignored,
  conditional and range answers, `CF-Cache-Status: HIT` on hits,
  `CELLD_CACHE_MAX_BYTES` cap (64 MiB default) with LRU eviction.
- `0019` (`856dba1`) context end aborts finished uploads: the
  caller runtime used to abort only pumps still registered at context
  end, so an upload whose pump already finished lingered sender-alive
  until isolate retirement; context end now aborts every upload id the
  context created, and the flow test asserts the host count is zero the
  moment the upload response lands. The conformance suites keep their
  fixture stores on tmpfs (a loud disk fallback where it is missing), so
  parallel suites on a loaded disk no longer stall lease commits into a
  node self-fence.
- `0020` (`72256ac`) `celld dev --port 0` assigns an ephemeral
  port: the node binds it and the supervisor learns the assigned address
  from the node's announce line, so parallel boots never race on a
  probed-free port; the conformance suites boot with port zero and
  discover the worker (200 on `/scope` or `/health`) and operator
  listeners via `/proc`.
- `0021` (`899897d`) tenant egress policy: `CELLD_EGRESS=public`
  refuses Worker `fetch()`/`connect()`/outbound-WebSocket dials to
  non-public addresses (loopback, private, link-local, CGNAT, ULA,
  unspecified, multicast, reserved, IPv4-mapped and transition-embedded
  forms, the metadata address), with `CELLD_EGRESS_ALLOW` CIDR/`host:port`
  exceptions matched exactly on every hop, enforced on the resolved
  address; tenant clients ignore proxies, celld's own traffic and
  published container endpoints are exempt, and refused fetches reject
  with `TypeError`.
- `0022` (`72456c2`) `celld d1 export` writes a consistent SQL
  dump: the owner node reads schema beside data under one transaction, so the
  dump is a single snapshot even under concurrent writes; `--table`
  (repeatable), `--no-schema`, and `--no-data` narrow it, and one dump
  carries at most 32 MiB. Export is operator-only: stub RPC to a D1 scope
  answers only the binding's `__d1Query`/`__d1Batch`/`__d1Exec`, and the
  export, migrate, and exec-with-rows fetch branches demand a host-set
  operator-ingress marker that only the signed `/runtime/` path sets. The
  dump streams schema and rows against the budget incrementally, skips
  generated columns, carries NUL and invalid-UTF-8 TEXT byte-exact,
  rebuilds fts5/fts5vocab/vec0 virtual tables without their shadows, and
  parses the virtual-table module instead of matching a DDL prefix.
- `0023` (`69114b9`) `celld d1 execute --batch --json` reads a JSON
  array of statements from stdin and runs it as one transactional batch:
  the cell runs the engine's own `batch()` behind a new fetch branch, so
  the CLI and the Worker binding share one implementation, and the CLI
  prints one JSON array shaped as the Worker's `batch()` shapes it.
- `0024` (`db25a35`) `celld d1 export` scopes each
  `sqlite_sequence` reset to its own counter: a filtered (`--table`) or
  data-only (`--no-schema`) dump replayed into an existing database no
  longer clears the AUTOINCREMENT counters of tables the dump does not
  name, so those tables cannot reissue the ids of deleted rows. A full
  dump still restores every counter exactly.
- `0025` (`dd83576`) port of meln1k/celld@ea471ee: a newer
  ownership release retires the in-flight one it supersedes, fixing
  the debug `indexed cell op N has no owning slot` panic and the
  release-build `cell_ops` leak; adds the
  `crates/logic/tests/regressions.rs` scripted-reproduction harness.
- `0026` (`ca46f61`) port of meln1k/celld@e46a685: fence
  settles startup confirmation reads, fixing the debug
  `confirming cell left the owner read in Fenced` panic on the
  halting event.
- `0027` (`589d893`) port of meln1k/celld@e6cb9a3: the alarm
  observer carries `WakeCoverage`, and wake maintenance reports
  `Confirmed` once the flusher says the current installation is
  covered, so a restored alarm no longer pins its cell resident
  forever.
- `0028` (`f4cd289`) port of meln1k/celld@6d61462: a reset
  settles an alarm handler still running without a barrier as a
  failed handler (re-armed, op retired, wake entry kept), fixing
  the `firing an alarm while not resident` panic on failed proofs.
- `0029` (`82503e2`) port of meln1k/celld@18d3bf9: the
  near-alarm eviction refusal applies only when the node has room
  to spare (no shedding, draining, or waiters), so a full node
  sheds instead of parking a waiter until its deadline.
- `0030` (`bebe38a`) port of meln1k/celld@9d4c163 (lib.rs
  hunks only; sim/docs files not ported): a drain counts every
  handoff against `max_releases` and its pump reads the live
  count; wake hints and alarm timers wait out a balancing move
  and fence clears the handoff mark; preserve settles a
  confirming read. Adds scripted reproductions for the drain
  ceiling, the handoff wait and clear, and the preserve settle.
- `0031` (`10075c8`) port of the two core fixes in
  meln1k/celld@468b456 (lib.rs hunks only; sim/docs files not
  ported): a reset drops the cell's sockets with its runtime,
  fixing the `live transport while not resident` panic, and a
  hibernatable socket's events wait for the restarted runtime
  on a quiescing cell, fixing the swap deadlock. Adds scripted
  reproductions for both.
- `0032` (`6ad1c53`) port of meln1k/celld@01522b3 (sim file
  not ported): a takeover from an expired lease that never opened
  a log waits for recovery, which CAS-seals the absent log so a
  stale session cannot open one through a renewal and ack into
  the superseded epoch; the seal refuses a lease that reads live.
- `0033` (`006104f`) port of meln1k/celld@b026226 (logic
  files only; sim/docs files not ported): 51 behavior-neutral
  code probes counting the core's rare branches, read through
  `State::probes`; prerequisite instrumentation for `0034`.
- `0034` (`f7cb761`) port of meln1k/celld@65fdb13 (logic
  files only; sim/docs files not ported): a cell whose last
  restore failed keeps its owner record — the drain and the
  balancer skip it — so no peer restores it from the bucket
  alone and loses the acknowledged tail; counted by the new
  `DormantKeptUnfolded` probe. Adds a scripted reproduction.
- `0035` (`77f722d`) wake-coverage follow-ups to the
  ported `0027`: the startup alarm observation is sent under
  the registry lock after the cell is registered, so a racing
  coverage report finds the handle; coverage reports compare
  alarm installations, not full snapshots, so a committed write
  advancing the cached position drops no `Confirmed`; and a
  coverage-only confirmation no longer cancels a non-drain
  eviction. Adds regression tests for all three plus
  `seal_absent_log` coverage; the exec-kill lib tests install
  the execution domain through the same shared test runtime,
  since a second `set_host_handle` would trip its
  exactly-once assert in one test binary.
- `0036` (`ff1d424`) concurrent requests share one isolate
  without stalling or borrowing each other's request: stateless entries
  carry continuation tokens so a shared promise's waiter keeps its own
  ops, the first turn installs its token for settling work, and event
  frames, request `/tmp`, subrequest budgets, and body-stream transfers
  resolve the running entry instead of the turn owner; request-owned
  sockets, WebSockets, HTMLRewriters and stream tees are accounted to
  the owning request, and a retired owner throws with nothing
  registered.
- `0037` (`8978fe4`) shared runtime cells run in a dedicated
  runtime isolate pool: cells of `__D1Database`, `__KvNamespace`, and
  `__Queue` build their isolates from the harness plus a fixed inert
  entry module, with no tenant bundle loaded — previously they ran in
  the first declaring script's pool beside that script's code. The pool
  reuses the cell-pool limits and memory accounting, carries the
  deployment-wide queue catalog, the node's routing identity, the
  generation tag, and the union of every declaring script's
  compatibility switches (so a storage capability only a cohosted
  script opts into, e.g. `sqlite_vec`, still applies to the shared
  cells it uses), and holds no tenant bindings, vars, services,
  loaders, or schedule. Deploy refuses a service binding whose target
  is not a valid script name, instead of failing at boot.
  Workflow cells and `.cron` stay in their own script's pool: they
  execute that script's `run()` and `scheduled()` with its bindings.
  Per-cell placement and ownership are unchanged — state stays in the
  cells' SQLite, so a rolling upgrade moves cells on restart with no
  migration — and `/state` reports the pool under its reserved name.
- `0038` (`14fab72`) container stdin close and signal reach the
  engine: both ops run under the start's lifetime (a fire-and-forget
  close no longer dies with its event; a signal lands even on failed
  cell events and stateless entries, the cases that dropped Handler
  work), so an exec with the default stdin reaches EOF instead of
  hanging. Single-shot engine control calls fail after 10 s instead of
  pinning their event on a wedged daemon, with a 60 s tier for container
  create/start/remove (large-layer unlinks and sandboxed starts
  legitimately take seconds); waits, streams, and image transfers stay
  unbounded. A start that fails after its create best-effort removes the
  container by name, and a failed post-start exec inspect kills the
  started process, so neither leaves an untracked orphan; the
  create/exec/wait retry loops share the 60 s aggregate cap. The
  websocket bind wait is unrefed: its reply is unobserved, so it no
  longer pins successful cell events.
- `0039` (`0423c14`) staged activation: `celld deploy
  --no-activate` uploads a version without moving any pointer; `celld
  activate SCRIPT=VERSION...` serves a multi-script generation with one
  atomic fleet-pointer write (service bindings resolve from the
  pointer's component map, queue attachments publish at activate time);
  `celld discard` removes versions no pointer names.
- `0040` (`f0fda85`) bindings resolve from the script's
  declared resources: cell dispatch and id derivation refuse names
  the calling script never bound (D1, KV, queues, workflows,
  Durable Object namespaces, reserved cells), runtime cells answer
  an RPC allowlist, and cell-scoped ops (storage, SQL, alarms,
  containers, facets, socket registry) answer only the executing
  cell. Stubs received over RPC or storage keep working through
  host-recorded grants: the host checks each lifted stub's brand
  and the sender's rights and holds the stub table itself, writing
  the envelope tag in Rust, and revive installs stubs only from
  that host-written table. Stored-stub rows written before this
  patch decode as plain data on first read; storing them again
  writes the new envelope.
- `0041` (`2037226`) the operator-only KV and Queue fetch
  branches demand the same host-set operator-ingress marker 0022
  introduced: every KV fetch op and the Queue info/purge/pause/
  resume/peek/redrive ops refuse stub.fetch() without it, while the
  bindings (RPC/send) keep working and host-driven queue settlement
  stays open. Stub RPC on the Queue and KV scopes answers the same
  per-class allowlist shape as D1: only the binding's
  `__queueSend`/`__queueMetrics`/`__kvGet`/`__kvPut`/`__kvDelete`/
  `__kvList` stay callable, and operator/internal methods refuse as
  not a function. The Queue purge/pause/resume/peek/redrive methods
  re-check the marker on the server-side request internally, so
  reaching them over stub RPC still refuses. The cell's own Queue
  DLQ forward rides the `__queue_dlq_forward` host op instead of
  stub RPC, and both `__queueSend` and the DLQ accept validate
  entries cell-side: the size must equal the body byte length, the
  content type must be known, bodies stay within bounds, and ids are
  host-minted. The tunnel markers are protocol-negotiated — a peer
  that sends no marker flag has its markers ignored, so a Worker on
  an older node cannot forge them — and the KV operator lookup is
  prototype-safe, with a null body refused as 400. DLQ and redrive
  need the fleet on this patch: mixed versions fail closed both
  ways (an old forward refuses against the new allowlist, a new
  forward errors against an old accept) and the alarm retries after
  convergence.
- `0042` (`75c73e7`) socket ids answer only their owner: a
  WebSocket id is usable only by the cell scope or request context
  that holds it (send, close, accept, hibernation list/tags,
  attachments, auto-response, isolate-pulled reads, and the
  registration ops), and TCP socket ids likewise by their event.
  Ids are unguessable (f64-safe random, nonzero); joining another
  cell's upgrade presents a single-use host-minted token, tunneled
  upgrade parks use unguessable numbers, and a reloaded cell still
  owns its sockets by scope after hibernation.
- `0043` (`852e15c`) a stream received over RPC stays usable
  while the receiver holds it: an active reader (or writer) keeps
  the stream past the receiving call and its invocation, the way a
  ReadableStream sent over RPC stays open until its writer closes
  it. Param disposal and context end spare a held stream; reading
  to the end, closing, cancelling, or an explicit dispose releases
  the hold at once, a dropped stream frees when collected, and
  isolate retirement cancels what a freed receiver still held (the
  revive records its heap). The sender side is unchanged: its
  invocation end still terminates the channel. Previously the
  receiving call's return cancelled a held abort carrier, so every
  signal-carrying exec aborted spuriously.
- `0044` (`db0c206`) `celld r2 head` and `celld r2 get` with
  `--json` print `{"error":"not_found","bucket":...,"key":...}` on
  stdout for an absent key, so callers match the `error` field instead
  of substring-matching the English message. The human message on stderr
  and the exit code are unchanged; `delete` of an absent key still
  succeeds, as on R2.
- `0045` (`4c8350c`) `CELLD_CACHE_SCOPE=deployment` shares
  one Workers Cache API scope across every script of the serving
  deployment: one `caches.default` and one set of `caches.open(name)`
  namespaces, like one Cloudflare zone. The default `script` scope
  keeps today's per-script entries byte-identical. The scope key is
  the fleet's bucket identity plus the serving deployment's identity,
  rendered by the host at build and stamped into tenant deployment
  scripts' isolate state only — the runtime pool (no tenant fetch
  handlers, not a script the pointer names) and dynamically loaded
  workers carry no key and stay script-scoped — so script input can
  never name another scope's entries. The scope comes only from the
  node's process environment, and an unknown value refuses the boot.
  Each scope carries its own size cap and LRU; a redeploy starts with
  an empty scope, a forced reload of the same deployment keeps its
  entries, and the old scope is dropped once its last serving
  generation drains. Sharing is total within a deployment (read,
  overwrite, purge), which is why it is opt-in.
- `0046` (`802ba54`) `celld --version` reports the full build
  version: `build.rs` bakes `CELLD_VERSION` (`<version>-ryuzcorp.<n>`,
  `<version>-ryuzcorp.ci` in CI) into the binary from the Docker build
  stage, and a plain `cargo build` without it reports the crate
  version. The build fails on any byte outside `[0-9A-Za-z._+-]`, so
  the value cannot inject cargo directives of its own.
- `0047` (`141cd5b`) `celld deploy --sourcemap PATH` collects
  the `no_bundle` entry's sibling `<entry>.map`: the entry deploys as
  written, so its caller-toolchain map still describes the deployed
  module. The deployed bytes are untouched. The sibling comes from a
  tree the caller controls, so it can neither fail the deploy nor reach
  outside the tree: the entry's directory must resolve inside the
  project root, the open refuses symlinks (`O_NOFOLLOW`) and blocks on
  nothing (`O_NONBLOCK`), the fd must be a regular file of at most
  64 MiB, and the JSON must be a v3 map. Anything else warns and writes
  nothing, as do a missing sibling and asset-only and Python projects.
  Its `sources` (with `sourceRoot` folded in) are rebased to
  project-root-relative paths lexically, never resolved on the host, so
  the map cannot probe which host files exist; absolute and URL sources
  keep their file name.
- `0048` (`ebcd078`) `celld invoke` sends one HTTP request to a
  staged script version on one node, served by that version's code with
  its declared bindings against the real data, so a migration endpoint
  runs before the activation that would serve it. `POST /invoke` on the
  internal listener requires the operator token plus the fleet HMAC; a
  shadow generation serves staged pins overlaid on the live map in
  fresh pools and retires after the request. Nothing serving moves.
- `0049` (sha TBD at merge) `celld invoke --header-env NAME=VAR` sends
  header NAME with the value of environment variable VAR, so a secret
  header (a proxy or migration token) never sits on argv, which every
  local user reads through `/proc/<pid>/cmdline`; the environment is
  readable only by the same user. An unset variable is refused without
  echoing anything, and the `--header` name and value checks apply.

Patches `0025`–`0034` are ported fixes from
[meln1k/celld](https://github.com/meln1k/celld) by Nikita
Melkozerov (Apache-2.0, like upstream): coordination-core
correctness fixes the author's deterministic simulator found,
ported without the simulator itself. Each entry above names its
source commit; the commits keep the original author with a
`Ported-from` trailer.

## Releases

- `v0.6.3-ryuzcorp.4` (`65419c6`): patches `0001`–`0006`; first
  release published by this fork.
- `v0.6.3-ryuzcorp.5` (`750e18a`): adds `0007`–`0008` (PR #2).
- `v0.6.3-ryuzcorp.6` (the commit that adds this entry): adds
  `0009`–`0042`. Upgrade notes:
  - Upgrade every node of a fleet together. A rolling upgrade moves
    cells with no migration (`0037`), but cross-version traffic fails
    until every node runs this release: operator calls through an old
    node fail closed on new nodes, Queue DLQ and redrive forwards fail
    in both directions, and stubs sent from an old node revive as plain
    data on a new one (`0040`, `0041`). The Queue alarm replays the
    durable DLQ and redrive marks once the fleet converges.
  - Storage rows holding stubs written before the upgrade read back as
    plain data (Durable Object stubs as null). Get each stub again from
    its namespace or binding and store it again to write the new
    envelope (`0040`); storing the read-back value does not restore a
    stub.
  - Workers now fail when they reach a resource their script never
    bound, touch another cell's storage, call D1, KV, or Queue
    operator methods through stubs, or use a socket id outside its
    owning cell or request (`0022`, `0040`, `0041`, `0042`). Declare
    every binding a Worker uses, and run operator actions through the
    `celld` CLIs.
  - The Workers Cache API takes up to 64 MiB of memory per node by
    default; `CELLD_CACHE_MAX_BYTES=0` turns it off (`0018`).
  - Shared D1, KV, and Queue cells move to a runtime pool that
    `/state` reports as `__celld-runtime` (`0037`).
  - Opt-in hardening stays off by default: `CELLD_EGRESS` is `open`
    (`0021`), `CELLD_FENCE_NODE_NETNS` is `off` (`0014`),
    `CELLD_MEMORY_SCOPE` is `container` (`0011`), and
    `CELLD_OPERATOR_TOKEN` is unset (`0013`).
  - Set `CELLD_OPERATOR_TOKEN` only once every node runs this
    release: older nodes send no token, so a mixed fleet with the
    token set fails internal calls with 401 (`0013`). Set it in the
    environment of every `celld` CLI too; liveness and readiness
    probes use the public health endpoint.
  - Queue rows whose dead-letter source was forged before the
    upgrade can still be redriven to the name they carry; planting
    such rows is closed by `0041`, redrive is operator-only, and
    retention ages them out.

## License

Apache-2.0, like upstream. `NOTICE` is kept, and every file the patches
modify carries a `Modified by ryuzcorp (ryuzcorp/celld)` notice naming
what changed, per Apache-2.0 §4(b).
