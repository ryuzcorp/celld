# ryuzcorp/celld

This is ryuzcorp's fork of [denoland/celld](https://github.com/denoland/celld).
Upstream ships only security fixes from 0.6.x on, so this fork carries the
feature patches we need on top of the pinned upstream releases. Upstream's
README below is kept intact; this file documents the fork.

## Branches

- `main` mirrors upstream `main` and carries no patches.
- `ryuzcorp/v<version>` is a line branch: the upstream tag `v<version>`
  plus our patches as commits, one commit per patch, in `0001`-`0057`
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
- `0049` (`3730834`) `celld invoke --header-env NAME=VAR` sends
  header NAME with the value of environment variable VAR, so a secret
  header (a proxy or migration token) never sits on argv, which every
  local user reads through `/proc/<pid>/cmdline`; the environment is
  readable only by the same user. An unset variable is refused without
  echoing anything, and the `--header` name and value checks apply.
- `0050` (`63e97a5`) `celld deploy` and `celld d1` read the
  project tree only inside the project's own directory: every
  project path is canonicalized and refused with an error naming
  its config key when it resolves elsewhere, while a link that
  resolves inside the tree keeps working (a link as the final `main`
  or assets-directory component stays refused, as before). Contained reads: the
  config file itself, the `main` entry (validation and every build
  path: pre-bundled, esbuild, Python), the unbundled-wasm scan, the
  Python sources and `python_modules/`, the `assets.directory` tree
  and its `_headers`/`_redirects`, `containers[].image` Dockerfiles
  and their build contexts, and `migrations_dir` for `celld d1`
  (whose entries must now be regular files, never links). A
  `containers[].image` that is an absolute or `..` path is refused
  rather than built; other non-project values stay image references.
  esbuild is contained too. Unlike Wrangler, the deploy may run with
  more privilege than the project's author, and esbuild reads whatever
  the sources point at: imports and `rules` modules through `..`,
  absolute paths or directory links, an input's `sourceMappingURL`, a
  `tsconfig.json` `extends` chain, the parent directories it searches,
  and it quotes a failing file's source line in its error output. On
  Linux it therefore runs under Landlock with reads confined to the
  project, its two output directories and its own executable (which must
  be the self-contained release binary), so an outside read fails like a
  missing file and reaches no bundle, map or error. Every run also writes
  a metafile, and the build fails unless every file esbuild read
  resolves inside the project: the whole guarantee for bundled files on
  a kernel without Landlock, where celld warns that esbuild runs
  unconfined. The Python SDK probe follows no link, so it cannot answer
  whether a host path exists.
- `0051` (`7ba9825`) `celld deploy --read-root DIR`
  (repeatable) names directories beyond the project that build inputs may
  come from: esbuild's inputs (the Landlock rule and the metafile check
  admit them) and `containers[].image` Dockerfiles with their build
  contexts. An absolute or `..` image value is a path, never a reference:
  it builds when it names a regular file inside a read root and is refused
  otherwise, joined lexically and probed only once it lands inside a
  bound. Every other project read (the config, `main`, assets,
  `migrations_dir`, Python sources) stays inside the project directory.
  Only the operator names a read root, on the command line, so a project
  cannot widen its own bounds. A platform that deploys a built
  subdirectory of a checkout, or a staged config whose Dockerfile lives in
  the checkout, names the checkout.
- `0052` (`8abf440`) `CELLD_DEPLOY_AWS_ACCESS_KEY_ID`,
  `CELLD_DEPLOY_AWS_SECRET_ACCESS_KEY`, `CELLD_DEPLOY_AWS_SESSION_TOKEN`
  give deployment-artifact reads their own credential. With the trio set,
  a node opens a second S3 client over the same bucket and serves
  `deploy/` and `deploy-blobs/` from it (the pointer, manifests, bundles,
  asset bodies, and container image tars), while every other key keeps
  `AWS_*`. A platform that mints a read-only grant over the deployment
  prefix and a read-write grant over the state prefixes can then hand the
  node both, so a credential copied out of the node's environment can
  read a deployment but never publish one. Access key and secret ride
  together, so a partial setting is a startup error for every invocation;
  a serving node also refuses the trio on a `gs://`/`az://` bucket, whose
  credential chain takes no static key. Unset keeps one credential for
  everything, exactly as before. The split belongs to the serving node only:
  `celld deploy`, `activate`, `discard`, and the other operator commands
  open the publishing client whatever their environment carries, because
  their writes must not go through a read-only credential. Every keyed
  bucket operation routes by key, so deployment artifacts read, list, and
  delete through the deployment client from one place rather than per call
  site.
  A managed node (`CELLD_CLOUD`, whose control plane deploys and prunes the
  pointer itself) refuses the trio at boot: its control-plane writes to
  `deploy/` would go through the read-only client and be refused, and so
  does every control-plane node with a BYO bucket, which runs the same
  deploy agent over `deploy/` and `deploy-blobs/`.
  The storage probe (`Bucket::validate` and the startup conditional-write
  probe) deliberately runs on the publishing client, because the deployment
  grant covers only the deployment prefixes; the state grant must keep
  read-write over `probe/` for a node to boot.
- `0053` (`6f6691e`) a serving process is non-dumpable: `main` calls
  `prctl(PR_SET_DUMPABLE, 0)` before it reads anything else, when the
  invocation is a node or the `celld dev` supervisor (and so the node it
  spawns; `execve` resets the flag, and every re-exec, including the
  control plane's credential rotation, comes back through `main`). A
  same-uid sibling — a tenant build running as the fleet uid — can then no
  longer read `/proc/<celld pid>/environ`, `mem`, or `maps`, or `ptrace`
  the node, so the fleet's `AWS_*`, `CELLD_DEPLOY_AWS_*`, and
  `CELLD_OPERATOR_TOKEN` values stay with the node. Operator commands stay
  dumpable. Linux-only; a no-op elsewhere. Cost: no core dumps from a
  node, and a non-root debugger cannot attach (root can). The flag holds
  from that call onward, not from `execve`: between the two the process is
  still dumpable, so initial boot, `celld dev`'s node respawn, and the
  control plane's credential-rotation re-exec each pay that window.
  Children a serving process spawns (`celld dev`'s esbuild, the presence
  agent's `hostname`) are dumpable again after `execve` but start with an
  emptied environment carrying only `PATH`, `HOME`, and `TMPDIR`, so none
  of the fleet's variables reaches them. The conformance suites read the
  node's announce lines from the `celld dev` supervisor's stderr instead
  of `/proc/<pid>/fd`, which a non-dumpable process no longer exposes.
- `0054` (`8bbd865`) a bucket-posture eviction costs fewer bucket
  requests. The eviction's visibility LISTs of `cells/<cell>/ltx/e<epoch>/`
  (the revocable handoff gate's and the one after the snapshot PUT) are
  skipped when this residency itself uploaded into that prefix and got a
  success response; a successor finds those objects by listing, as every
  restore does (list-after-write consistency, see `docs/guarantees.md`). A
  residency that uploaded nothing itself (a resumed local
  copy, recovered tails, bundles) and the L0-chain fallback still list. The
  capture that `handoff_wait` and the handoff snapshot run skips the
  time-based checkpoint (`Db::sync_before_close`), whose sealing write
  committed a content-free txid that the eviction uploaded as its own L0
  just before the snapshot covering it; the size-based checkpoints still
  run, and a cancelled eviction checkpoints at the next ordinary sync.
  Measured on 200 short-lived Durable Objects (6 writes each,
  `CELLD_DURABILITY=bucket`): 12 Class A requests per cell before, 9 after
  (per 200 cells: `cells/*/ltx` PUTs 1600 → 1400, LISTs 600 → 200).
- `0055` (`dc51ff2`) a lone fleet node slows its posture-repair
  scan: while a `maintain` pass finds no eligible peer, the wait before the
  next pass doubles from one second up to the maintenance tick (thirty
  seconds) instead of retrying every second forever. Each pass lists
  `nodes/` and reads every lease, so the flat retry cost a lone node about
  2.6M LIST plus 2.6M GET a month doing nothing. `maintain` now reports
  whether the pass found no eligible peer, and only a node that never
  installed an ensemble backs off on it: a degraded ensemble that lost
  all its followers still retries at once, so it re-recruits the moment
  one returns. Every other branch behaves as before, and the first
  repair after a degrade is still immediate, a peer that loses to the
  drain barrier still retries at once, and the epoch step still doubles
  to ten seconds. Recruitment is advisory — without an
  ensemble every write already waits for a bucket proof — so the slower
  scan only delays fleet posture, never RPO=0 or fencing. The tradeoff: a
  node that starts next to a lone node is recruited by it within about
  thirty seconds instead of about one (the new node recruits the old one
  itself at once).
- `0056` (`4d9955a`) concurrent bucket-posture writes share
  ownership reads instead of reading once per write. A gated write's
  bucket proof used to emit its own `VerifyOwnership` (one GET of
  `cells/<cell>/own.json` per write); now a proven gate joins a read in
  flight on its cell and epoch whose cover (the issuing gate's own
  position) reaches its position, and issues its own read at once
  otherwise, so no write waits longer than before. Positions strictly
  increase with every committing write, so a joiner committed before the
  issuer and the issuer's completed proof captured it. A write at a
  position a verified proof already covers releases with no new proof or
  read. The read is always issued after a proof that captured every gate
  it answers, a failed or refused read fails every gate it answered and
  resets the cell exactly as before, fleet proofs never share reads, and
  results for drained gates (fence, deadline, reset, epoch change) are
  ignored.
  Measured on one hot cell with 16 concurrent writers for 15 s against
  RustFS with 40 ms store latency (`CELLD_DURABILITY=bucket`, debug builds
  of the base and of this commit): GET `cells/*/own.json` 1550 → 553 at
  the same throughput (1549 → 1514 writes). A first draft that queued
  past-cover gates behind the read in flight cut the reads to ~180 but
  lost a third of the throughput, so it was dropped.
- `0057` (`e199983`) an eviction deletes the per-cell L0 and L1
  objects its handoff snapshot supersedes. After the snapshot PUT succeeds,
  a background task deletes this epoch's journaled upload keys with max
  txid at or below the snapshot's — the keys this residency itself uploaded
  (sync L0s, compaction L1 outputs, same-epoch recovery uploads) — with no
  LIST, one single-key `DeleteObject` per key (never bulk `POST ?delete`:
  R2 prices only single `DeleteObject` as a free operation, S3 single
  DELETEs are free) at bounded concurrency (8), abandoning the rest on
  stop; the eviction itself returns at once and needs no deadline. The
  snapshot and other epochs are never candidates, the L0-chain fallback
  deletes nothing, and failed or abandoned DELETEs leave restore-invisible
  garbage for epoch GC without failing the eviction. The restore planner
  anchors on the snapshot and only extends past it, so a deleted object is
  in no restore plan whenever its DELETE lands — including after release
  and a successor's activation, which always takes a new epoch and prefix
  (`next_writer_epoch`; the only same-epoch path, `resume_local`, needs
  live local files that eviction removes and uploads only strictly-new
  keys). The journal is capped at 8192 entries per cell epoch (≈200 KiB);
  beyond that uploads stay unjournaled as restore-invisible garbage.
  Measured on the same 200 short-lived Durable Objects: stored `cells/`
  bytes 2.47 MB → 0.57 MB, objects 1800 → 400 (L0 1400 → 0; the 200
  snapshots stay).
- `0058` (`f7877ed`) a reproducible S3 request-cost harness in
  `tools/s3bench/`, so cost claims in PRs can be re-measured instead of
  quoted. `proxy.py` is a threaded HTTP reverse proxy in front of the S3
  endpoint logging one JSON line per request (method, path, query, status,
  bytes in/out) with optional `DELAY_MS` latency; it forwards Host as the
  client sent it, so SigV4-signed requests still verify upstream.
  `count.py` classifies the log into `METHOD x key-family` counts with an
  R2 Class A/B summary (listings count as LIST by their `prefix` param;
  bulk `POST ?delete` stays POST, Class A — only single-key DELETEs are
  free). `stored.py` inventories stored
  `cells/` objects and bytes per family. `app/` is a minimal Worker +
  SQLite-backed Durable Object app with `/agent/<id>/write` and
  `/hot/write` routes. `run.sh` creates the bucket in a local
  `rustfs/rustfs` container (bucket names lowercase; create accepts 200 or
  409 and aborts otherwise), starts the proxy and N celld nodes
  (`CELLD_BIN`, ports offset by `OFF`) pointed at it, deploys the app,
  drives the agent workload (or one hot cell with `HOT=1`), prints the
  counts and the stored inventory, then kills its proxy and nodes and
  removes its container on exit. Measured with a debug build of the tip:
  agent workload (`1 200 60 10 60 120`, bucket, 30 s evict) 1200 ok turns,
  PUT `cells/*/ltx` 1400, LIST `cells/*/ltx` 200, GET `cells/*/own.json`
  1400, PUT `cells/*/own.json` 200, DELETE `cells/*/ltx` 1200, stored
  `cells/` 400 objects / 581188 bytes; hot workload (`HOT=1 DELAY_MS=40`,
  16 writers, 15 s) 1489 writes, PUT `cells/*/ltx` 259, GET
  `cells/*/own.json` ~520. New files only, so no `Modified by` notice;
  nothing under `tools/` runs in CI.
- `0059` (`2719952`) the sans-I/O core for bucket-posture group commit
  (layer 1; no executor or fold wiring yet). `BUCKET_FORMAT` becomes 3, and a
  bucket node writes shared bundle proofs only while `bucket_bundles_allowed`
  (every live lease reads format 3; a single-node fleet's own lease opens it,
  no live lease at all stays closed). Each feature judges the fleet against its
  own minimum — `PAGED_FORMAT` (2) for the paged-epoch gate, `BUCKET_BUNDLES_FORMAT`
  (3) for bundles — so the bump never closes paged epochs on a mixed fleet. The
  node-log record gains a bucket-session flavour (`SessionKind::Bucket`, wire key
  `"bucket"`; old records read as fleet, old binaries ignore the key) with a
  `create_bucket_record` constructor for the record-before-first-ack rule;
  `takeover_gate` already recovers-first on any live record and is pinned so a
  bucket session is never complete unless sealed; credit
  still demands `Open`-at-epoch after the PUT (`bundle_credit_allowed`) and the
  seal still demands an empty retained-bundle scan (`graceful_seal_allowed`). Two
  pure predicates for the layer-3 fold — `may_declare_bounded_loss` and
  `requires_fleet_witness` — are always false for bucket sessions and preserve
  today's fleet behaviour exactly. Proven fail-closed for a format-2 reader: it
  decodes the new record (unknown key ignored) and either folds the whole bundle
  prefix with no loss declaration or refuses the fold; never complete, never a
  dropped row. Downgrade safety rests on that recovery-correctness, not on refusal.
- `0060` (`5172069`) the bucket-posture executor (layer 2): a bucket node with
  an open gate installs a registration without an ensemble, publishes its bucket-session
  record through the live-session lease chain before the first bundle-credited ack (never
  deleted, never `active == true`), and `bundle_active` fires with no shipper. Reuses
  `put_bundle` verbatim (PUT → bucket re-read → `bundle_credit_allowed` → credit or
  degrade); credit advances the same `synced_seq`/`durable_txid`, so acks return
  `ProofSource::Bucket` behind the unchanged `own.json` ownership read. Gate: bundles only
  while `bucket_bundles_allowed` (off until the first fleet sample, re-checked per flush;
  `main.rs` mirrors the paged gate) AND `CELLD_BUCKET_BUNDLES=1` — OFF by default, so a
  partial stack never ships enabled until layers 3-5 merge. Gate closed (or credit refused
  after a seal/zombie) falls back at once to per-cell L0 uploads for uncredited rows.
  Group commit is event-driven: flush at once when dirty and no PUT is in flight, again as
  soon as one completes, on a dedicated `Notify` the sync loop cannot steal; optional
  `CELLD_BUCKET_BUNDLE_LINGER_MS` (default 0, cap 1000) holds a round open to batch more;
  capped by rows/bytes and split above the cap; fleet path unchanged. Bucket posture only
  (`CELLD_DURABILITY=bucket`). `maintain` never converts a bucket session to an ensemble; no
  posture watch or recruit on bucket posture. Layers 3-5 still pending (successor fold of
  bucket bundles, drain/GC/graceful seal for bucket sessions) — do not enable in production.
- `0061` (`d77d1c2`) the eviction guard proves a journaled epoch with a
  HEAD on the newest journaled key instead of a prefix LIST (parity item (iii),
  layer 1). `ObjectStoreClient::ltx_file_exists` HEADs the `ltx_key` the
  uploader wrote and the 0057 trim deletes — one constructor shared by all
  three, pinned by a round-trip test — and `epoch_replicated` tries it first,
  falling back to today's LIST on a miss, an error, or an empty journal;
  refusal on LIST-empty is unchanged. The agent workload's 200
  `LIST cells/*/ltx` are the claim-time lost-record check (`ltx_exists`), a
  cold path with no journal by construction, so they keep the LIST; every
  warmer guard (non-self-uploaded residencies such as recovery uploads and
  compaction output, the L0-fallback barrier) drops from one Class-A LIST to
  one Class-B HEAD, proven by unit tests (warm guard: 1 HEAD + 0 LISTs; empty
  journal: LIST kept; HEAD miss: LIST fallback, then refuse). No name,
  format, or protocol change.
- `0062` (`8bbdd74`) parity item (iv) layer 1, core-only: the per-round
  fence credit predicate plus the output-gate settlement it feeds. `log_tier`
  gains a two-arm credit check per flush-round cell — the session-record arm
  (`bundle_credit_allowed`, still `Open` at the shipper's epoch after the PUT)
  and the per-cell fence arm (the entry read after the PUT still names this
  node at this cell epoch) — with the proof-then-read ordering witness
  (`ProofThenRead`) part of the verdict: a fence read issued at or before the
  proof never credits. The reader abstraction is the `RoundFence` trait (one
  outcome per cell: entry, missing, or read error), so layer 2 can back it
  with per-cell `own.json` reads now and shard reads later without touching
  the predicate. The gate settles `Event::FenceRoundVerified` with no
  per-gate `VerifyOwnership` read: credited cells release proven, uncredited
  cells fail exactly like a refused ownership read (fail, never hang), and an
  unordered round fails what it covers. No `crates/celld` change yet, so no
  runtime behaviour changes. Fail-before per arm: a sealed record crediting
  (record arm), a live-donor bump crediting (fence arm), a pre-proof read
  crediting (ordering) each turn their test red.
- `0063` (`ec27f09`) the successor fold for bucket-posture group commit
  (layer 3): a bucket session's acked rows are restorable after death, restart,
  or a quiet handoff. `recover_serial` branches on the session kind — bucket
  passes skip the member loop outright (no `post_tail`/`post_seal` RPCs for a
  session that has no ensemble) and judge loss with
  `may_declare_bounded_loss` (fleet guard unchanged: the witness conjunct
  stays; the predicate is never true for bucket, so a bucket pass drains
  bundles and never declares); the whole-prefix bundle gather, windows,
  `upload_gathered` and the seal are byte-identical. `fold_cell` branches the
  same way: the witness demand is `requires_fleet_witness` (fleet behaviour
  identical, bucket never demands a follower tail) and the live donor
  session's (`<node>/<generation>`) retained uncovered bundle rows for the
  cell fold before the restore, keeping the snapshot-after-observations
  ordering. Routing is pinned, not changed: a live matching owner lease gives
  `Route::Remote` and never a claim/takeover effect, so no path takes a
  single cell from a live bundling donor without whole-session recovery
  (new regression test in `crates/logic/tests/regressions.rs`). Six plain
  `#[cfg(test)]` recovery tests (no sleeps): whole-node death plus successor
  sweep, crash between bundle PUT and per-cell coverage via the takeover
  interlock, same-node restart via `recover_self`, the live-session fold,
  and corrupted-marker variants pinning both predicates. `ensure_recovered` /
  `recover_self` wiring needed no change. This is the read side of the L2 gap
  (takeover and restart now fold bundle-credited rows per-cell instead of
  orphaning them); the eviction/handoff/drain side is layer 4. Still behind
  `CELLD_BUCKET_BUNDLES=1` — do not enable in production until layers 4-5.
- `0064` (`644041e`) drain, GC, and graceful close for bucket sessions
  (layer 4; still behind `CELLD_BUCKET_BUNDLES=1`, still do-not-enable until the
  stack completes). Eviction of a bundled cell writes ONE per-cell snapshot from
  the complete local DB — never a per-cell L0 re-upload — and the guard proves
  durability through that snapshot (`residency_uploaded`), so no LIST and no
  per-cell L0 at eviction; the fallback keeps its LIST and refuses when neither
  artifact lands. The revocable gate no longer LISTs for (or refuses) cells
  whose acked rows sit outside the per-cell layout (`durable_txid` past per-cell
  coverage), fixing the measured "no replica objects" refusals. The snapshot
  feeds the same `covered_txid` watermark the fleet path uses, so
  `bundle_deletable` sees it. `gc_bundles` runs for bucket sessions (maintenance
  tick, no ensemble) and deletes only fully-covered bundles with single-key
  `DeleteObject`s, never bulk `POST ?delete` (free on R2, as the 0057 trim).
  `close_gracefully` seals a bucket session only when every acked row is
  per-cell: it first snapshots each bundled cell within the shutdown budget,
  and otherwise leaves the record Open for successor recovery. The bucket
  linger (`CELLD_BUCKET_BUNDLE_LINGER_MS`) holds the window open only while
  more than one cell is dirty — a lone cell flushes at once, since waiting
  would add the whole linger to one ack without batching anything.
- `0065` (`d7fd9f2`) trim the C heap sooner after eviction: the
  sampler tick returns glibc free pages past `CELLD_C_TRIM_MB` (default 8,
  was a 32 MiB const), with a pure `trim_due` predicate and unit tests.
  Measured post-drain RSS 72.0 → 61.9 MiB on the agent-50 s3bench shape. A
  jemalloc arena purge/decay was tried and dropped: log-verified, it freed
  only ~1 MiB of ~20 MiB slack (the rest is active-run fragmentation
  `mallctl` cannot return), and the post-drain "counting" bump is the
  graceful-shutdown drain's working set, not retention — no fix needed.
- `0066` (`7e870d4`) a per-connection SQLite page-cache budget:
  `finish_open` sets `PRAGMA cache_size=-<kb>` from `CELLD_SQLITE_CACHE_KB`
  (default 2000, the stock ceiling: no behavior change at merge). Knob
  validated at startup and documented in `docs/`.
- `0067` (`fc0646b`) share the `/bundle` tree per deployment
  generation instead of per isolate: the projection lives on the shared
  `Arc<WorkerConfig>`, so N isolates project once; read-only semantics
  unchanged. Counter-based unit test proves one projection per config.
- `0069` (`016541d`) the s3bench agent workload drives its agents
  concurrently: one thread (and connection) per agent, each writing every
  `write_every_s` seconds on its own phase (`start + agent*every/agents`),
  so arrivals are uniform like independent Noite/Outer clients and
  cross-cell group commit can batch. The serial walk kept at most one cell
  in flight and could never batch. The expected turn count comes from the
  schedule (every round due within the run), not from what fired, so a
  round truncated by the deadline or a slow write fails the run as a
  shortfall. Per-cell behaviour does not depend on concurrency, so the
  measured counts match the serial driver. The README notes that agents
  are concurrent and suggests `DELAY_MS=100` for realistic batching numbers
  (R2 answers a PUT in ~100–200 ms from a VPS).
- `0070` (`3c442a1`) rollout for bucket-posture group commit
  (layer 5): group commit is ON by default in bucket posture.
  `CELLD_BUCKET_BUNDLES` defaults to on and `=0` disables it; any other
  value stops startup (`env_vars::validate`). The format-3 fleet gate still
  governs: bundles are written only while every live lease reads format 3,
  off until the first fleet sample, re-checked per flush with instant
  per-cell fallback. Default linger stays 0; `CELLD_BUCKET_BUNDLE_LINGER_MS`
  (cap 1000) holds a round open only while more than one cell is dirty, so a
  lone cell never waits. Defence in depth: the lease publish path asserts
  its own advertised `bucket_format == BUCKET_FORMAT` before the first
  publish (a fleet-split build fails loudly instead of silently closing the
  gate). Docs: takeover-gate and bucket-proof paragraphs in
  `docs/guarantees.md` (record-before-first-ack, sealed-means-per-cell,
  gate rule, downgrade-by-recovery, L4 eviction snapshot), the two knobs in
  `docs/README.md` and `celld -h`. Measured on the concurrent agent
  workload (1 node, 200 agents x 6 writes, `DELAY_MS=100`), Class A
  PUT/POST/LIST (L4 reference OFF 1934 / ON linger 0 1120 / ON linger 200
  986): OFF 1934, default (no switch) 1136 with bundling engaged, linger
  200 982 — 1200/1200 turns every run. Kill-and-restart with the default
  config (concurrent workload, kill -9 at T+25s): 692 acked turns, every one
  restorable (200/200 probes, chains intact). Downgrade drill: the new binary
  acked 1200 turns into bundles and was killed; the `.10` image binary (fresh
  watch dir, different node, same bucket) took every cell over, folded the
  whole bundle prefix with no loss declared, and served 200/200 probes with
  every acked row restorable.
- `0071` (`01a68fb`) fix the bucket hot-cell double-write, and quiet
  idle bundle GC. A `sync_loop` worker spawned before the bundle arm kept
  uploading per-cell L0s beside every bundle flush: a continuously-dirty
  cell never looks clean, so the worker never left its loop and every row
  landed twice (hot shape: 132 per-cell PUTs beside 65 bundles, Class A 262
  vs `.10` 190). The worker now checks `bundle_owns_tiering` — the scan's
  `sync_should_bundle` predicate with the same inputs — each iteration and
  yields once bundling owns tiering; the bundle loop credits the same
  watermarks, and the next scan re-spawns the worker if bundling drops. A
  round whose dirty set is a single cell with a single pending ticket
  uploads that cell per-cell through `sync_cell` (journal, uploaded flag,
  credits, waiter notify — the gate-closed fallback's function, no second
  convention) instead of a one-cell bundle: the same PUT count, minus the
  record re-read, the GC delete and the bookkeeping; a lone cell with more
  pending rides a one-cell bundle as before, because the hot shape is one
  cell with many concurrent writers and per-cell would serialize one PUT
  per round and never batch (measured: 135 per-cell PUTs and zero bundles). A
  failed lone-cell upload falls back like a failed bundle PUT: the stall
  drops bundling so the scan re-spawns a sync worker that retries with
  backoff, instead of parking the round with the scan suppressed. A lone
  round whose sync claim is held (a pre-arm worker about to yield, whose exit
  nudges only the suppressed scan) rides a one-cell bundle instead of
  skipping, so the ticket never waits for the next write.
  Fleet posture is untouched (followers need the bundle rows, so the paced
  path keeps batching single cells into node bundles). `gc_bundles` skips
  its LIST once a full-prefix scan leaves zero bundles remaining and no
  bundle PUT was attempted since — any attempt, including a failed or
  ambiguous one, re-arms; confirmed deletes drain the count; the first scan
  after boot, recovery or takeover always LISTs; fleet sessions never skip
  (a fenced ex-leader's rows share the prefix). An idle bucket node cost one
  LIST per 30 s tick (~2880 Class A a day that `.10` never paid); now it
  costs one discovery LIST and then nothing until it writes. Tests:
  `bundle_loop_uploads_a_lone_cell_per_cell`,
  `bundle_loop_shares_one_bundle_between_two_cells`,
  `bundle_loop_bundles_a_hot_lone_cell_with_pending_writes`,
  `lone_cell_per_cell_failure_heals_through_retry`,
  `lone_cell_round_with_a_held_claim_still_credits`,
  `bundle_mixed_history_evicts_through_one_snapshot`,
  `gc_bundles_quiet_until_a_put_re_arms` (plus the 0064 eviction/gate/GC
  tests it stacks on). Fail-before: every lone-vs-shared direction reverted
  (always-bundle, always-per-cell, no pending gate) turns its test red; the
  stall reverted parks the failed lone-cell round and its await times out;
  skipping a lone round on a held claim parks its ticket the same way;
  the gate skip reverted refuses all five bundle eviction tests with "no
  replica objects"; the stale check reverted snapshots a stale image; each
  GC-quiet arm reverted (no skip, no PUT arm, no delete drain) turns the
  quiet test red at its own assert. Measured (concurrent harness,
  `DELAY_MS=100`, default config, 1 node): hot (16 writers, 15 s) 563 ok /
  0 failed, Class A 140 (`.10` 190, `.11` 262); agent-200 (200 agents x 6
  writes) 1200/1200, Class A 1137 (`.10` 1934); agent-50 (50 agents x 6
  writes) 300/300, Class A 571 (`.10` 555, +2.9% residual), Class B 633
  (`.10` 619, +2.2%). The agent-50 A residual is structural at linger 0,
  not a regression: the core (350 per-cell PUTs + 50 own.json + 50 LISTs)
  and the fixed overhead (104) equal `.10` exactly, and the excess is 13
  stray one-cell bundles plus 4 GC LISTs. At 5 arrivals/s with a
  sub-200 ms loop every write needs its own artifact PUT — per-cell and
  one-cell-bundle PUTs swap 1:1, re-reads are Class B, deletes are free —
  so batching more would need a linger, which default config refuses (a
  lone cell never waits). 555 is zero bundle ops plus one LIST, i.e. a run
  with no multi-dirty capture at all; the floor with jitter is ~566-572.
- `0072` (`f1f15c9`) the s3bench agent and hot drivers report
  per-turn latency (`mean`, `p50`, `p95`, `p99`, nearest-rank, whole ms),
  timed per request in the worker's own thread after its pacing sleep and
  recorded only for 2xx answers. Pass/fail semantics are unchanged; the
  numbers make ack-latency regressions visible next to the request counts.
- `0075` (`630cedc`) group commit defaults OFF in bucket posture.
  Release measurements show bundling roughly doubles durable-write p50 even
  with the pipelined credit re-read, so `.11` ships per-cell uploads: an
  unset `CELLD_BUCKET_BUNDLES` now disables grouping and only an explicit
  `1` enables it (`0` still disables; any other value, including an empty
  one, still stops startup via `env_vars::validate`). `docs/README.md`
  (grouping paragraph, env table), `celld -h`, and `docs/guarantees.md` now
  describe grouping as opt-in and state the measured trade-off; the
  guarantees themselves are unchanged. Measured at `DELAY_MS=100`, release
  builds, 1 node: agent 200x6 Class A 1934 -> 1140 with grouping on, but
  write p50 210 -> 445 ms; a hot cell with 16 writers 235 -> 432 ms p50
  with ~45% fewer turns. Enable it for request-cost-sensitive workloads
  that tolerate about one extra round trip per durable write. Tests:
  `bucket_env_is_off_unless_explicit_one`. Fail-before: the old
  `!matches!(Some("0"))` value enables with no env set.
- `0076` (`a88d2bc`) s3bench measures release builds and one run per
  box: `run.sh` defaults `CELLD_BIN` to `target/release/celld`, and refuses
  to start while another `celld-s3bench-*` container is up, unless
  `S3BENCH_ALLOW_PARALLEL=1` (counts-only runs). Two `.11` measurement
  errors motivated it: debug binaries compared against the `.10` release
  image, and peer harnesses running concurrently, which skewed hot-cell
  latency by up to 40%. Checked by hand: with a `celld-s3bench-*` container
  up the script exits 2 before creating a work dir. Tools only; CI does not
  run `tools/`.
- `0077` (`b3f864a`) every black-box test boots `celld dev` on an
  ephemeral port and reads the bound address from the node's own announce
  lines. Cause: all 12 test files probed `free_port()` (bind `127.0.0.1:0`,
  read the port, drop the listener) and only later bound `--port <port>`;
  in between, a parallel test could take the port — a sibling node's
  ephemeral internal listener or another test's probe — so the child
  exited 1 while `GET /version` still answered from the squatter (PR #66
  run 43799719504 on arm64: `staged_invoke`
  `invoke_refuses_unknown_and_malformed`, "celld dev exited during boot:
  exit status: 1"). Fix: boot with `--port 0` and take the public address
  from the `celld listening on <addr> (` stderr announce line inside the
  existing boot-poll deadline; the shared `forward_announce`/`announced`
  helpers live in the new `crates/celld/tests/common/mod.rs` (the
  standard Cargo pattern: `tests/common/mod.rs` is not its own test
  binary), used by all 12 files including `staged_invoke` and
  `operator_token`; an early child exit panics with the collected stderr.
  The same class in unit-test form —
  `policed_connect_reports_tcp_failure_for_allowed_hosts` dropped its
  `TcpListener` probe before dialing — now binds a `tokio::net::TcpSocket`
  without listening and holds it for the whole test, so the refused dial
  stays refused and the port stays reserved (no new dependency). No
  fail-before test fits: the race is scheduling-dependent. The
  deterministic evidence is that the old `free_port` port could be taken
  between probe and bind, while the new code never names a port.
- `0078` (`fb97ac5`) evictions skip the handoff snapshot for a short
  chain born in this epoch (MergeTree-style lazy handoff). Motivation: every
  eviction captured and PUT a full-DB L9 snapshot, then trimmed the journaled
  L0s it superseded — an eager full merge on every eviction. In bucket
  posture without group commit (Noite's and Outer's config) every write is
  already a per-cell L0, so a cell born in this epoch has a complete
  restorable chain from TXID 1 and the snapshot is pure overhead: 200 of the
  1934 Class A requests on the s3bench agent-200 shape are eviction snapshot
  PUTs, and every one rewrites the whole DB. ClickHouse MergeTree, the model
  here, writes small parts on insert and merges them lazily by count and size
  policy; it never merges just because an event happened. Policy: decline the
  snapshot exactly like `snapshot_declined`/paged (compaction pause held, run
  lock taken) iff the chain starts at TXID 1 in this epoch's prefix with
  every object journaled here (fresh cells and `.evicted`/`.hibernated`
  reuse, whose generation starts at TXID 1; remote restores, paged
  continuations, same-epoch resumes and uncertified local rows keep the
  snapshot), the per-cell prefix covers every acked row, and this epoch's
  upload journal holds at most 16 objects (`HANDOFF_CHAIN_MAX_OBJECTS`: a
  successor restore is then at most 16 parallel GETs) without ever filling.
  Safety: the skip ends in the existing L0-chain fallback (`sync_wait`
  Durable, then `epoch_replicated`) with no new proof and unchanged refusal;
  no trim runs without a published snapshot (`trim_max` stays `None`); the
  snapshot bytes are unchanged whenever the policy keeps it; older binaries
  restore the result as any L0-chain handoff today. A skip logs
  `eviction_snapshot_skipped` with cell, epoch, object count and reason. The
  skip's final pass reads the local position without syncing: it goes straight
  to `epoch_replicated` only if the WAL holds nothing past the capture (the
  exact `Db::has_uncaptured_changes` probe — `Db::pos` cannot see a WAL-only
  tail, and a blind check dropped such a row) and the per-cell prefix covers
  every local commit; only a real pending commit runs `sync_wait` as today,
  so neither the due time-based checkpoint nor `ensure_wal_exists` can mint a
  content-free txid as its own L0 just before the handoff (measured: one extra
  PUT per eviction on cells older than the 60 s interval). The
  decision is pure (`crates/logic/handoff.rs`, `handoff_snapshot_needed`),
  unit-tested there including the 16/17 boundary; `ltx_repl` gathers the
  inputs. Tests in `ltx_repl`: a fresh short chain with its checkpoint due
  skips (no snapshot PUT and no sealing L0, `L0Chain`, nothing deleted, the
  next epoch restores the rows); a write after the gate with no waiter is
  synced as one L0 and restored by the next epoch; an `.evicted` reuse skips
  again; a restored cell, a 17-entry journal, and a durable-ahead-of-coverage
  cell still snapshot. The probe is unit-tested in `celld-ltx` (clean after a
  sync, dirty after an unsynced commit, clean after a timed checkpoint with no
  new writes). Fail-before: forcing always-snapshot makes the fresh test
  publish one snapshot PUT and return `Snapshot`; forcing the sync path makes
  it upload one sealing L0; blinding the probe drops the WAL-only row.
  `drain_bundled_cell` is untouched. Measured at `DELAY_MS=100`, release
  builds, 1 node, `CELLD_IDLE_EVICT_S=30 CELLD_TTL_MS=60000`, all turns ok:
  agent-200 lazy PUT ltx 1200 / Class A 1734 / p50 209 against `.11` 1400 /
  1934 / p50 210 (the 200 snapshot PUTs gone, latency unchanged); agent-50
  lazy 300 / 532 / p50 224 against `.11` 350 / 582 / p50 223. The skip leaves
  the L0s in place (agent-200: 1200 objects until epoch GC, against 200
  snapshot objects) instead of trimming them behind a snapshot; single-key
  DELETEs are free, so the request saving is the whole win.
- `0079` (`9805b72`) cancelled requests reject shared work instead of hanging co-awaiters (upstream #317).
  A stateless request that disconnected while its subrequest was in flight
  dropped the op's resolver, so every other request awaiting the same promise
  hung to the 300s handler budget with nothing logged. `reject_pending_ops`
  now rejects the refed operations the event still waits on — the fetch
  wrapper carries the rejection to each co-awaiter's catch — and drops the
  unrefed ones: their awaiters re-subscribe in a later event (a retained exec
  read re-armed under a later subscription). Timer ops cannot reject into
  callback-style `setTimeout`, so the harness fires an armed timer's callback
  when its op dies with the event; the value the callback computes is
  unchanged, only early. Shutdown keeps the old silent drop. Tests:
  white-box, two entries sharing one promise driven through `turn_cancel`,
  plus the `retained-disconnect` black-box conformance (a disconnecting
  request with a pending unrefed retained wait drops it; rejecting it would
  settle the record fatally).
- `0080` (`d4304d7`) pin shared-promise op attribution for waiter-owned next ops (upstream #316).
  No runtime change: the issue's `celld dev` repro (B awaits A's shared fetch,
  then arms its own timer/fetch) already answers on this tree, because the
  0036 continuation-token line attributes B's next op to B's driver and hands
  it over instead of adopting it into A. The new white-box test drives two
  entries sharing one promise through a real timer firing and asserts the
  resuming entry's driver adopts nothing foreign. The `shared-waiter`
  black-box conformance drives the issue's repro shape end to end (B awaits
  A's shared slow fetch, then arms its own timer/fetch); with the adopt-owner
  filter broken B hangs to the 15 s read deadline, with it the run answers in
  about a second.
- `0081` (`2d4a84f`) answered drivers poll while waitUntil background is outstanding (upstream #300).
  An answered entry driving waitUntil work with no native op of its own
  concluded it waited on nothing: the cell branch returned `Idle` at once, so
  `stuck()` abandoned the background and retired the registering event, and the
  stateless branch spun on its tick without ever observing the settlement. A
  later event resolving the awaited promise then resumed a continuation whose
  origin context was dead: storage transactions failed with "the cell event
  ended before it could acquire an input gate", and the error report over the
  same socket failed with "the event that issued this operation already ended",
  so the client saw nothing. Both branches now poll while background work is
  outstanding, keeping the registering event alive to lend its context and take
  the handoff; for an entry with a request id, the answered empty-ops branch
  samples shutdown cancellation before polling, so a lifecycle cancellation
  still retires the background entry instead of spinning past it to the
  budget (only shutdown interrupts the poll once answered). An answered entry
  without a request id does not sample shutdown in that branch; the background
  budget (`0101`) still bounds it. Tests: `waituntil-tx` black-box conformance drives
  the issue's WebSocket start/complete dance and asserts both transactions
  commit and read back (times out on the old code, answers in milliseconds
  with the fix), plus the `shutdown-retire` conformance (a lifecycle
  cancellation retires an answered entry with pending background work).
- `0082` (`7cb31af`) pin cancelled-request lazy-init settlement for Durable Objects (upstream #294).
  No runtime change: a Durable Object lazily starts a timer shared through
  `this.ready`, the first request is cancelled mid-flight, and the retry must
  answer. The cancel path fixed for #317 is shared with cell entries (the cell
  driver reaches the same `turn_cancel`), so the dropped timer op rejects, the
  harness fires the armed callback early, and the retry answers "timer fired".
  Tests: `cancelled-lazy` black-box conformance boots `celld dev`, waits on
  the `/started` handshake until the object's lazy init provably started (well
  before the 60 s timer — the handshake, not a sleep, decides when the abort
  lands), aborts one `/timer` request over TCP, and asserts the retry answers;
  on pre-fix code the retry hangs onto the read deadline.
- `0083` (`430bf06`) retire with the mailbox drained so late handoffs rejoin the drive (upstream #309).
  A driver could retire with a handed-but-untaken op still queued: another
  event's turn enqueued after this driver's last take but before its exit check,
  and `finished()` only looked at the adopted set. The op's future was then
  dropped with the mailbox while its JavaScript promise pended forever. Both
  drive loops now take the mailbox once more before leaving — closing it in
  the same turn, so a handoff racing the take is refused at the source instead
  of landing after it unresolved. Refused handoffs and trailing takes reject
  refed ops and drop unrefed ones (a retained exec read re-armed under a
  retired owner's slot follows the unrefed path); this fixed a regression in
  `rpc_stub_response_body_across_events`. Tests: `abort-timer`
  black-box conformance drives the issue's start/abort/status dance and asserts
  "done"; it wedges pre-fix. Trailing handed ops for finished, non-keeping
  events now reject (via `turn_reject_trailing_handed_ops` with the
  refused-handoff message): `finished_non_keeping_event_rejects_trailing_handoff`
  and `trailing_take_closes_mailbox_so_late_handoff_refuses` in
  `retired_event_attribution` (integration placement — the same lib-suite
  SIGSEGV hazard as the #289 test); each fails with its path gutted and passes
  with it. The refused-handoff half is pinned white-box for #207 below.
- `0084` (`d46df02`) pin refused-handoff settlement for retired owners (upstream #207).
  No runtime change beyond the #309 trailing-take: the issue's trace shows a
  mailbox accepting cross-request work and then retirement discarding it when
  cleanup removes the promise resolver. The trailing-take now delivers handoffs
  that land during exit, and refused handoffs — owner truly gone or mailbox
  already closed — reject instead of dropping the resolver. Tests: white-box in
  `retired_event_attribution`; a stale continuation's timer op names its retired
  owner, the releasing turn refuses the handoff, and the promise settles.
  Verified failing on pre-fix code.
- `0085` (`ba66236`) pin answered waitUntil retirement with no native ops (upstream #289).
  No new runtime change beyond the #300 wake path: the stateless driver used to
  sleep-loop once an answered entry's own ops ran out, so a waitUntil aggregate
  settled through another event was never observed and the entry never retired,
  pinning old generations after a deploy. The wake path now polls while
  background work is outstanding; `settle()` observes the settlement and the
  entry retires. The ignored lib test is gone: in-process V8 turns SIGSEGV a
  later manager test, so `answered_waituntil_without_ops_retires_once_background_settles`
  in `retired_event_attribution.rs` drives both entries through the real driver
  on one pool-built isolate via public API only. Pre-fix the answered entry's
  wake spins to the 300 s budget and the 30 s deadline fires; post-fix both
  drives finish in milliseconds. The same-branch `waituntil-tx` black-box
  conformance covers the cell side of the fix in CI.
- `0086` (`93d8e09`) pin constructor task loop resumed by a later event (upstream #277).
  No runtime change: a task loop started in the constructor without keeping that
  event alive, resumed by a later request, then fetching and storing, reports
  the expected `[awaited, fetched, stored]` on this tree — the
  continuation-token and cross-entry-gate line already roots the woken work.
  Tests: `continuation-resume` black-box conformance pins the shape (fetching
  the node's own `/plain`, so no external network) so a future attribution or
  retirement regression fails loudly instead of wedging the object.
- `0087` (`ba81a8a`) pin woken-continuation outbound settlement (upstream #258).
  No runtime change: a request starts async work and returns, a later request
  wakes the continuation, which then fetches or writes. The reported hang is
  already gone on the base tree — the dead origin's ops refuse at creation and
  the waiter settles loudly, while the self-woken control still succeeds.
  Tests: `woken-outbound` black-box conformance drives both legs and asserts
  every result settles instead of hanging. Full workerd parity — running the
  woken call under the live waker instead of refusing it — needs the
  live-ambient adoption design and stays open.
- `0088` (`187ac7a`) treat a stale-target 409 as proof the member is gone (upstream #273).
  The seal path could not tell a dead member from transport noise. Only a 409
  `WrongTarget` carrying the peer-auth gate's stale-route marker counts as "the
  old member is gone"; every other seal failure stays inconclusive and retries
  as before. The denial lives in one shared helper (`peer_auth_denied` in
  `main.rs`) so the seal/tail route cannot drift from the abort route; `Replay`
  is also a 409 and must not carry the marker — a replay proves nothing about
  the member, and marking it would seal away intact rows. Wire-level tests
  serve real seal requests through `internal_log` over a socket. Tests:
  `stale_target_409_proves_member_gone`,
  `other_seal_failures_stay_inconclusive`.
- `0089` (`767080a`) bound the renewal lease readback to remaining authority (upstream #264).
  The renewal readback hung on a stalled store and wedged every renewal until
  the fence timer, even with proven headroom left. `Effect::ReadSelfNodeLease`
  now carries `authority_expires_ms` taken from the prior record, bounding the
  read to the remaining authority. Tests:
  `a_renewal_readback_carries_the_authority_bound`.
- `0090` (`d63de1b`) first alarm-deadline expiry only marks the firing (upstream #256).
  The operation deadline firing while the alarm copy was still running retired
  the op at once, re-firing while the first copy ran. The first expiry now only
  marks `Firing { expired: true }`; the running copy settles the alarm when it
  answers, so a slow handler is never doubled. A second expiry with the op still
  outstanding treats the op as lost and retires it exactly as a failed handler
  would, keeping alarms at-least-once instead of turning a stuck handler into a
  lost one. Tests: `an_alarm_deadline_waits_for_the_running_copy`,
  `a_twice_expired_alarm_still_refires`.
- `0091` (`effe445`) read follower tails under a progress timeout (upstream #250).
  Follower tail reads ran under one whole-request timeout. The body now streams
  under a 10 s progress timeout (`TAIL_PROGRESS_TIMEOUT`; connection setup
  stays bounded by a 5 s connect timeout), which matches the old whole-request
  timeout for a stalled peer while a slow-but-moving transfer completes. The
  send rides the same budget: the stream client carries no whole-request
  timeout, so a peer that accepts TCP then stalls before the response headers
  would hang `post_tail` forever. The transfer beats the recovery claim while
  streaming (`post_tail_with_beats`): a healthy tail slower than the heartbeat
  still holds its claim when it lands, instead of reading 30 s stale.
  Tests: `steady_chunks_complete`,
  `stalled_stream_fails_at_the_progress_budget`,
  `stalled_response_headers_fail_at_the_progress_budget`,
  `slow_tail_holds_the_recovery_claim`.
- `0092` (`7d57736`) pin the node-session id in the data dir across restarts (upstream #249).
  Every restart generated a fresh node-session id, so a restarted process
  stranded its predecessor's leases and fragments behind `WrongTarget`. The id
  is now pinned in a `node-id` file beside the follower fragments, so the id
  and the disks it recovers share a fate: an explicit `CELLD_NODE` wins
  outright and is recorded for the next boot, otherwise the boot reuses the
  pinned id, generating and pinning one on first boot. A missing or unwritable
  directory degrades to today's ephemeral id with a loud line, never a boot
  failure; a corrupt pin warns and regenerates, naming the discarded id for
  operator recovery. The pin lands durably (tmp file, fsync, rename, dir
  fsync; a stale tmp from a crashed boot is consumed). `celld -h` documents
  the default as "generated and pinned to node-id in the data dir", reused
  across restarts on the same data dir — the PID-scoped default temp dir
  mints a new id every boot, so set `CELLD_WATCH` to a persistent directory
  to keep the id. Tests: `explicit_id_wins_and_is_recorded`,
  `restart_reuses_the_pinned_id`, `corrupt_pin_regenerates`.
- `0093` (`8de22b3`) retry transient CAS conflicts with the same guard (upstream #239).
  Every transient store conflict (HTTP 409) on renewal paid a full readback
  round trip out of the remaining authority for an ETag the readback would only
  re-learn. Renewals now retry twice with the same guard
  (`MAX_SAME_GUARD_CONFLICT_RETRIES = 2`, AWS's documented behavior), then fall
  back to the readback like a lost race; an initial acquisition with no prior
  authority always reads back. Tests:
  `a_lease_conflict_retries_with_the_same_guard`.
- `0094` (`fd5ef12`) declare loss for a deleted session with surviving objects (upstream #245).
  A restarted leader moves its lease to the new generation and a dead session
  never opens a new epoch, so the old generation's record reads back absent and
  the sweep skipped its sealed fragments forever. An absent record now counts as
  closed — the fragments are garbage no new epoch will ever adopt — and the
  sweep writes a `log/<node>.loss.json` record naming the leader, so the loss
  is bounded and loud instead of silent; a session that never existed seals
  absent quietly with no record, and a read error still retries next pass. The
  seal mark extends as before, so a straggling append is refused rather than
  resurrected. The record marks the first declaration: its `declared_at_ms` /
  `declared_by` are the forensics of when the loss was found, so every later
  pass over the still-deleted session keeps them (a conditional create wins
  the first write and loses later ones). `declare_absent_loss` retries a
  create that hits a 409 conflict with the same guard (up to 3 attempts with
  backoff): only a 412 lost race proves another pass declared it, so a still
  conflicting record falls back to a GET and fails closed. Tests:
  `deleted_session_with_objects_declares_loss` (asserts the loss object parses
  and names the leader), `never_existed_session_seals_absent_quietly`,
  `loss_record_keeps_the_first_declaration`.
- `0095` (`01a7ad1`) resume a degraded same-member ensemble without an epoch step (upstream #268).
  Every pass over a degraded ensemble stepped the epoch even when the eligible
  membership still matched the record, churning records while a sick member
  healed. A degraded ensemble whose membership matches its record now resumes
  its epoch; past `MAX_SAME_ENSEMBLE_RESUMES = 3` consecutive same-ensemble
  resumes the loop steps the epoch anyway, so a member that lost its fragment
  can still rejoin. Residual: under a persistently failing append verdict the
  pattern is three cheap resumes and then one full step — a lease write and
  posture repair per fourth consecutive degrade — so the loop still pays about
  a quarter of the pre-#268 lease-write rate instead of zero. The guard
  mitigates the 429-to-self-fence churn; it does not eliminate the step while
  the failure persists. A degraded resume additionally requires the folded
  record to name the shipper's epoch and ensemble (`folded_names_shipper`):
  only a match may clear `degraded`. Tests:
  `degraded_same_ensemble_resumes_without_an_epoch_step`,
  `resume_budget_steps_past_three`.
- `0096` (`b3bc3ea`) sleep until the next upcoming alarm minute, not the next tick (upstream #308).
  The periodic scan slept to the next 60 s tick, so a persisted alarm fired up
  to a tick late. `partition_minutes` splits armed minutes into the due set and
  the next not-yet-due minute, and the caller sleeps until that minute instead.
  Tests: `due_and_next_upcoming_split`, `no_upcoming_without_future_minutes`.
- `0097` (`3a4a474`) stable node ids, witness grace, loss record, lease conflicts, alarm deadline, wake timing (upstream #244).
  Docs only: `docs/guarantees.md` covers the pinned node-session id and witness
  grace, the `log/<node>.loss.json` loss record, the same-guard lease-conflict
  retry and authority-bounded readback, the mark-first alarm deadline (the
  no-duplicate guarantee covers only the first expiry — a second expiry with
  the op still outstanding retires and re-fires, preserving at-least-once),
  and the wake-for-the-upcoming-minute timing. No code change, no tests.
- `0098` (`f36e6e2`) retain local L0/epoch LTX files after compaction (upstream #311).
  After a successful L1 fold the node deleted local L0 files the fold covered,
  including the newest file `pos()` and verify still read back. Deletion now
  keeps the newest covered file, and each activation plus each node startup
  drops live-named epoch dirs a newer local epoch supersedes, skipping resident
  replicas; the clean-reload path already pruned to its exact resident set, so
  the startup sweep is a no-op there and every other boot restores from the
  bucket. Tests: `retain_l0_through_keeps_newest_covered_file`,
  `superseded_live_epochs_pruned_at_startup_and_activation`.
- `0099` (`440fdf2`) dev-store single-writer group commit (upstream #310).
  Concurrent PUTs piled onto `objects.sqlite3` with one connection and one
  `Immediate` transaction each, losing the SQLite lock lottery into
  `busy_timeout` sleeps: aggregate write throughput collapsed toward one fsync
  per write, and unlucky uploads stalled seconds past the durability budget
  (`DurabilityUnproven` under fan-out, plus renewal starvation that can
  self-fence the node). Every PUT now funnels through one writer thread, one
  connection, and one queued batch per transaction. Draining whatever is already
  queued costs nothing when idle (a batch of one is today's single PUT) and
  shares one commit fsync under load; each row keeps its own savepoint so
  create/update conflicts still fail alone. A panicked writer falls back to a
  direct PUT rather than failing every write. The writer thread holds only the
  batches counter, never a channel sender, so dropping the last store handle
  disconnects the channel and the thread exits (its handle is joined). Local
  dev-store only; fleet and bucket durability paths are untouched. Tests:
  `writer_batches_one_burst_into_few_commits`,
  `writer_batch_preserves_row_conflicts`,
  `concurrent_conditional_puts_stay_correct`.
- `0100` (`cce8f3b`) repair 0.5.1 facet-image import (upstream #292).
  The 0.6.0–0.6.2 import tested `page_count == 0`, but under replication the
  replica opens the facet file first and its control tables already push the
  count past zero, so the import never ran: silent upgrade data loss. The check
  is now no-user-data, which also repairs the files those versions left behind.
  The image replays through ordinary SQL writes, which commit beside a live
  replica read lock (a `Backup` would take `EXCLUSIVE` and fail), and the
  decision is stamped in `user_version` so a later wipe never resurrects it.
  Live file content always wins over the legacy image, which is safe because
  0.5.1 never wrote facet files; the legacy row stays for manual recovery. One
  corner remains: an all-content wipe made on 0.6.0–0.6.2 followed by this
  upgrade replays the image once — delete again on the fixed version. Tests:
  `status_tracks_marker_then_content`,
  `status_ignores_names_and_sequences_but_counts_writes`,
  `replay_restores_rows_schema_and_index`.
- `0101` (`2212df2`) bound post-response background work (upstream #275).
  A handler that answered but left timers or waitUntil pending held its drive's
  affiliation until the event finished, so a retiring generation's isolates
  never drained: each deploy leaked a heap until shedding latched
  `CapacityExhausted` forever. Past the background budget the driver ends the
  background work without failing the answered request, and the abort pass
  releases the isolate. New knob `CELLD_BACKGROUND_BUDGET_S`, default 30
  seconds — Cloudflare's waitUntil limit. An event that still holds the cell's
  input gate or runs another event's critical section keeps its ops by design,
  and stays loud about it. `expire_background` clears `background_since`, so a
  gate-holding event past budget warns at most once per budget window. Tests:
  `default_matches_cloudflare_waituntil_limit`,
  `override_applies_and_restores`, `expired_background_re_arms_its_clock`,
  plus the `retire-leftover` black-box conformance.
- `0102` (`1fd0c1d`) keep an O(1) hydration count (upstream #270).
  A full walk of the cut per faulted read made a whale's fill quadratic in its
  size (111 min for 8.9 GB) and stalled foreground reads behind the walk's
  mutex hold. The hydration set now carries an exact cut-relative count,
  maintained on every insert and truncation, so `hydration()` is constant time.
  Live file content still always wins over fault data, and the count never
  covers the lock page or past-cut holes. Tests: unit tests in `paged_vfs`
  covering the count against an oracle walk, clean-after-sync,
  dirty-after-unsynced-commit, and clean-after-timed-checkpoint.
- `0103` (`6fad8e9`) collect fragments of record-less sessions (upstream #259).
  A follower dropped a sealed fragment only when its record said so, but a
  restarted leader moves its lease to the new generation and a dead session
  never opens a new epoch: the old generation's record reads back absent, and
  the sweep skipped it forever. An absent record now counts as closed — the
  fragments are garbage no new epoch will ever adopt — while a read error still
  retries next pass. The seal mark extends as before, so a straggling append is
  refused rather than resurrected. Absence now distinguishes terminal cases —
  no lease at all, or a lease whose generation superseded this session — from
  a live lease that simply has no log yet (before its first publication, or a
  slow first-open racing fragment arrival): only the terminal cases collect,
  a live logless lease's fragments are kept. Tests:
  `sealed_session_fragments_are_collected_without_a_new_epoch`,
  `sealed_generation_scoped_fragments_are_collected`,
  `superseded_generation_fragments_are_collected_without_a_record`,
  `deleted_lease_fragments_are_collected_without_a_record`,
  `live_logless_lease_fragments_are_kept`.
- `0104` (`36c498c`) restart the cell after SQLite destroys a transaction (upstream #291).
  A write failing with `SQLITE_FULL`, `IOERR`, `NOMEM`, or `INTERRUPT` destroys
  its transaction, and every later event on the cell — reads included — failed
  until something reopened the database. Only an eviction recovered the cell.
  The actor now restarts the runtime when a finished activity leaves its scope
  broken; the swap pump carries the restart on the current generation, and the
  placement opens the database on a fresh isolate, which clears the poison. The
  mark lives only in memory and a reopen clears it, so this is downgrade-safe.
  Tests: `full_disk_poisons_until_reopen` plus the `disk-full` black-box
  conformance.
- `0105` (`e391da1`) prune live-named epochs after an unclean restart (upstream #290).
  A node that lost its DO to another node kept the dead live-named files
  forever after a crash or a `handoff=preserve` stop, rotting outside the
  local-cache budget. At boot after predecessor recovery — unless the boot is a
  clean reload — the node now deletes every live-named epoch, since only a
  clean reload resumes one and a restart claims each cell at a new epoch from
  the bucket. The deleted cells stay in a take-once boot list that the local
  reload replays, so ownership confirmation still finds the cells that were
  resident when the predecessor died. Tests:
  `unclean_restart_prunes_live_keeps_snapshots`.
- `0106` (`08b983b`) unref `AbortSignal.timeout` so it frees the cell slot (upstream #306).
  A signal created by `AbortSignal.timeout()` held its cell slot past its
  usefulness. The timer is now unref'd in `js.rs` and the harness. Tests:
  the `abort-timeout` black-box conformance (70 requests, past the 64-request
  `DEFAULT_MAX_CELL_REQUESTS` admission limit the test boots with, each
  minting an unused 60 s timeout signal to prove unreferenced timers never
  hold a slot).
- `0107` (`f1df460`) carry the 101 webSocket target across loader fetch (upstream #305).
  The upgrade target was lost when the loader fetched, so the 101 response had
  nowhere to route. The target now crosses the loader fetch through
  `http_streams`/`js`/websocket routing. Handoff quotas are keyed by
  deployment (the host `script_name`, or `__loader:{id}` for loader-loaded
  workers) plus scope, so loaded workers sharing the empty routing scope do
  not share one bucket: 64 per quota, 1024 globally, share-aware
  displacement, 60 s expiry sweeps first. The forwarder lives in
  `celld::isolate_pipe`: `local_isolate_websocket_pipe` runs each direction
  in its own task with a shared shutdown, and a producer-side `WsPipeBudget`
  charges frames at admit — pre-registration `pending` included — against
  16 MiB staged per direction (`ISOLATE_PIPE_MAX_STAGED_BYTES`);
  `register_budgeted` adopts a pre-existing budget with its used count and
  tripped state instead of charging the backlog twice. The first refused
  data frame queues a `WsOut::Overload` marker behind everything admitted
  ahead of it, and the tripped budget refuses every later data frame
  (closes and the marker bypass), so the pipe ends loudly instead of
  parking silently; every consumer turns the marker into an unclean 1011,
  and the staged-cap `OverLimit` path closes both ends the same way,
  winning over a raced close. A close waits behind queued data with a
  per-frame 5 s progress bound (each forwarded frame restarts the wait).
  The pull endpoint survives teardown until its terminal is consumed or
  the request retires. A cell- or facet-owned bind's target endpoint keeps
  a host-set `cleanup_scope` on its pull entry instead of a re-owner (the
  re-owner broke `op_ws_next` for loaded workers polling outside the
  joining cell and hid frames behind a synthetic 1006): read authorization
  stays with the request owner, while cell shutdown reclaims the endpoint
  by scope (`ws_pull_unregister_cell`, a tree sweep that takes the dying
  root's facet scopes along via registry metadata, run before output
  unregister removes that metadata); only request-owned binds track the
  target for request-retirement close. A caller-initiated close reaches
  the caller on both pipes instead of reporting an abnormal 1006. Tests:
  `ws-loader` fixture plus `ws_loader_upgrade_conformance`.
- `0108` (`5a7db54`) build external stubs from the union of main + sibling imports (upstream #304).
  External stubs were built from the main module's imports only, missing ones
  only a sibling imported. The loader now unions main and sibling imports.
  Tests: `loader-imports` fixture plus `loader_imports_conformance`.
- `0109` (`d735aeb`) populate `process.env` from worker vars under `nodejs_compat` (upstream #296).
  Under `nodejs_compat` the Worker's `vars` now populate `process.env` before
  the module evaluates (host value wins; the harness and module loader carry
  it through); `nodejs_compat_do_not_populate_process_env` opts back out. A
  dynamically loaded Worker's loader-supplied strings reach the handler's
  `env` and — under `nodejs_compat` — `process.env` alike, while the
  host-owned `CELLD_NODE` name never crosses into `process.env`. Dangerous
  keys are skipped: `__proto__`, `constructor`, and `prototype` never cross.
  Tests: `env-probe` conformance (`worker.js`, `wrangler.json`, plus the
  force-added `.dev.vars` fixture the folded fixup supplies, since the root
  `.gitignore` excludes real `.dev.vars` secrets),
  `loaded_worker_env_populates_process_env`, and the `__proto__`-smuggle pins.
- `0110` (`1b42f82`) support AES-KW wrap/unwrap plus symmetric JWK import/export (upstream #295).
  `SubtleCrypto` gains AES-KW key wrapping/unwrapping and `raw`/`jwk`
  import/export of symmetric keys; a large JWK export builds its binary
  string in 32 KiB chunks instead of one spread. AES JWK keys must be 16,
  24, or 32 bytes; SHA-1 HMAC exports as `HS1` (reimported under that name);
  a JWK's `alg` (when present) must match the key, and a present AES `use`
  must be `enc`. AES-KW needs an AES-KW wrapping key, and the RFC 3394
  minimum sizes are enforced (one 8-byte semiblock is not wrappable;
  off-shape lengths stay rejected). Tests: `aes-kw` fixture plus
  `aes_kw_conformance` (including `single_semiblock_is_rejected`).
- `0111` (`8ff2708`) import/export EC public keys in raw point format (upstream #271).
  `SubtleCrypto` imports and exports EC public keys in uncompressed raw point
  form alongside the existing formats. Tests: `ec-raw` fixture plus
  `ec_raw_conformance`.
- `0112` (`cf98b29`) writable request `/tmp` files and `url` file helpers (upstream #280).
  Requests get writable `/tmp` files (`node:fs` over a per-request dir) and
  `node:url` gains the file-URL helpers (`new node_url.js`, plumbed through
  the harness and module loader). New knob `CELLD_TMP_MAX_BYTES` (default
  67108864, 64 MiB; validated positive): each new entry costs `path.len() +
  64` bytes against it, and any write, append, or stream chunk that would
  push a request past it fails with `ENOSPC` — an oversize write is refused
  before its bytes are copied — while an overwrite frees the replaced bytes.
  Also new: `appendFile`, `appendFileSync`, and `fs.promises.appendFile`;
  `writeFile` honours `{ flag: "a" }` (and `wx`/`ax` fail with `EEXIST` while
  `a` appends; read-mode flags are refused), and `createWriteStream` writes
  chunks as they arrive so the cap applies during the stream instead of at
  close. Write streams honour `encoding` for string chunks (unknown encodings
  refused up front) but refuse `start`; read streams refuse `start`/`end` /
  `encoding` — all with `ERR_INVALID_ARG_VALUE`. A stream opens its file
  before emitting `open` (`final` never re-truncates the file `open`
  initialized), and recursive `mkdir` charges each missing prefix as it goes
  (no half-created prefix on refusal) while a recursive `mkdir` on a file
  returns `EEXIST`. `/tmp` files report writable (`W_OK` passes, mode
  `0o100666`). `fileURLToPath` accepts `%5C` (only an encoded slash is
  rejected), and `pathToFileURL` keeps a directory's trailing slash so the
  URL round-trips. Tests: `tmp-fs` fixture plus `tmp_fs_conformance` (cap,
  overwrite/append accounting, streaming cap, cross-request isolation via
  `ENOENT`, outside-`/tmp` `EPERM`).
- `0113` (`0449525`) accept workerd's sendable close codes so `server.close` delivers (upstream #279).
  The harness enforced the WHATWG-only rule (1000 or 3000–4999) and threw
  `InvalidAccessError` on workerd's sendable application range — refusing the
  1008 policy-violation close dropped a Worker's refusal on the floor: the
  listener threw and the client hung. An endpoint may now send 1000–4999 except
  the reserved no-status codes 1004, 1005, 1006, and 1015, which stay refused.
  This changes default close-code behavior: codes 1001–1014 (other than the
  reserved ones) that used to throw now close. A caller-initiated close is
  delivered on both pipes (direct and cell-targeted joins), so the caller
  sees the close it sent instead of an abnormal 1006. Tests: `ws-close`
  fixture plus `ws_close_conformance`.
- `0114` (`2568d44`) transparently decompress gzip/deflate response bodies (upstream #238).
  `fetch()` responses with `Content-Encoding: gzip` or `deflate` are now
  decompressed before delivery, chunk by chunk: a truncated body is an error
  (reported as a `response stream` error), not a clean end, and multi-member
  gzip concatenates. Tests: `gzip-decode` fixture plus `gzip_decode_conformance`.
- `0115` (`20f638f`) attach `Symbol.dispose` to plain object and array results (upstream #260).
  RPC results that are plain objects or arrays now carry `Symbol.dispose`, so
  `using` declarations clean them up like other RPC handles — on pure-data
  results the dispose is a no-op (a bare clone with no table to revive).
  Tests: `rpc-dispose` fixture plus `rpc_dispose_conformance`.
- `0116` (`ea1d58a`) serve `dir/index.html` at its trailing-slash URL (upstream #318).
  The static asset server mapped a directory URL to its `index.html`; the
  trailing-slash form missed. Both now serve the index. Tests:
  `static-dir-index` fixture (root and `sub/` indexes) plus
  `static_dir_index_conformance`.
- `0117` (`ca53a67`) route a post-response DO dial by its object's cell (upstream #254).
  A Durable Object opening a WebSocket after its response was already sent dialed
  without its object's cell attached, misrouting the connection. The dial now
  carries the object's cell. Tests: `do-later-ws` fixture plus
  `do_later_ws_conformance`.
- `0118` (`078cd14`) publish the abort synchronously on caller cancel (upstream #281).
  Cancelling a loader call published the abort a turn late, so work the caller
  had already abandoned kept running into the next turn. The abort is now
  published synchronously (a `DoCallCancels` registry in `js.rs`). A loader
  fetch dropped before dispatch releases its cancellation state (the dispatch
  guard disarms right after spawn), while aborts after dispatch still
  propagate. Tests: `loader-abort` fixture plus `loader_abort_conformance`
  (including `retired_dispatch_clears_a_published_abort`).
- `0119` (`38afd3f`) refuse durable object bindings with a foreign `script_name` (upstream #312).
  A binding carrying another script's `script_name` was counted as a class of
  the deploying script, so the node boot-looped reporting the class as exported
  by more than one co-hosted script. `celld deploy` now refuses such bindings
  and names `script_name`; a binding naming its own script stays local. This
  changes deploy behavior: projects that declared a foreign `script_name` and
  previously booted (or boot-looped) now fail fast at deploy time. A
  non-string `script_name` is refused the same way instead of being treated
  as absent. Tests:
  `durable_object_binding_with_foreign_script_name_is_refused`,
  `durable_object_binding_with_own_script_name_is_local`,
  `durable_object_binding_with_non_string_script_name_is_refused`.
- `0120` (`60cfb61`) upload image tars multipart instead of one PUT (upstream #307).
  A container image tar rode one PUT under the client's 30 s single-try
  timeout, so any GB-scale image failed deterministically and retries could not
  help. `Bucket::put_multipart_bytes` splits the tar into 8 MiB parts, each on
  its own request, and `ensure_image_tar` uses it. The timeout itself is
  untouched: it is a correctness condition for the node self-fence, not tuning.
  Tests: `failed_complete_aborts_the_session` (a failed `complete` aborts the
  multipart session instead of leaking its parts) plus bucket unit tests
  driving a conflicting store through the multipart path.
- `0121` (`a1e6d11`) add `--local` to run execute, export, and migrations against `celld dev` (upstream #272).
  `celld d1 execute`, `export`, and migrations gain `--local`: the dev node's
  local store carries its leases and shared secret like a fleet bucket carries
  its fleet's, so discovery and request signing work unchanged through a
  bucket-backed entrance. `--local` resolves the project through the
  canonicalized config (`DevState::project_dir`), so a symlinked
  `wrangler.json` resolves to the config's own directory — and names its
  canonical `.celld/dev`, not the link's parent — opens
  `PROJECT/.celld/dev` (following the `--state`/`CELLD_DEV_STATE` override),
  and posts to the running dev node, which must be up. Combining it with
  `--bucket`/`--endpoint` (or `CELLD_BUCKET`) is a refusal, so migrations can
  never run against the wrong database by a silent bucket choice. `d1` also
  takes `--state DIR` (resolving like `celld dev`: flag, then
  `CELLD_DEV_STATE`, then the project); `--state` without `--local` is
  refused. Docs: `docs/services/d1.md`. Tests:
  `local_without_dev_state_names_celld_dev`, `local_state_flag_parses`,
  `state_without_local_is_refused`,
  `local_state_flag_selects_the_state_directory`, and the d1 CLI suites.
- `0122` (`4695c31`) answer local LISTs from a covering index over a prefix range (upstream #265).
  `LocalStore::metadata` read every row and filtered the prefix in Rust, and the
  listed columns sat behind body overflow pages, so each list read the whole
  `objects.sqlite3` — about 21 s per cold cell on a 4.7 GB store. The open now
  creates the covering `objects_listing` index and bounds the scan to the prefix
  (`key >= prefix AND key < prefix-upper`, `CAST` back to `TEXT`); the Rust-side
  prefix check stays as the semantic backstop. The listing statement lives in
  `listing_sql`, EXPLAINed by `listing_plan_uses_the_covering_index`, which
  requires a bounded SEARCH with the prefix upper bound — a regression to the
  unprefixable scan fails it. No new knob; dev-store only.
- `0123` (`8f20767`) add `celld versions` to list deployed versions (upstream #181).
  Rollback (`activate`) and prune (`discard`) existed but there was no way to
  list previous versions. `celld versions [SCRIPT]` lists every
  `deploy/<script>/<version>/manifest.json` from one LIST, marking the serving
  generation current from the fleet-wide pointer: the top-level version,
  staged components, and — under a pointer without a components map — every
  named-pointer service version serving resolves bindings through, matching
  what `discard` refuses to delete; an optional `SCRIPT` narrows the rows.
  Preview access stays out of scope: serving an old version against live
  cell state needs a contract this command does not invent. New CLI:
  `celld versions`.
- `0124` (`694395b`) add `dev --state DIR` for local state outside the project (upstream #302).
  `celld dev` always kept state in `PROJECT/.celld/dev`, where backups and
  search tools pick it up. `--state DIR` (or `CELLD_DEV_STATE`, default
  `PROJECT/.celld/dev`) moves it anywhere else; `d1 --local` resolves through
  the same rule, so it follows the override with no flag of its own. The
  watcher ignores the `--state` dir subtree, and the watch root is
  canonicalized first: event paths otherwise arrive uncanonicalized past a
  symlink and miss the canonical state prefix, reviving the loop that the
  ignore was added to stop. `--clean`
  over an explicit directory deletes only when the marker `prepare_state`
  writes proves celld created it: a missing directory is already clean, and
  `--state` refuses to adopt a non-empty foreign dir (fail closed, contents
  intact) — and an explicit path ending in `.celld/dev` is not exempt: the
  suffix cannot prove celld created it, so it needs the marker too. An empty
  or whitespace-only flag or variable counts as unset, not a directory.
  `d1.md` lists the flags `--local` refuses (`--bucket`, `--endpoint`,
  `CELLD_BUCKET`, `S3_ENDPOINT`). New CLI flag `--state` and new env knob
  `CELLD_DEV_STATE`.
- `0125` (`b88dad7`) drop the budget-orphaned drive when a retry arrives (upstream #299).
  A workflow retry arriving after the old drive was budget-orphaned left both
  drives alive. The retry now drops the orphaned drive; an in-memory redrive
  bit (`isRedrive`, threaded through `__fireAlarm`) marks the orphaned
  workflow drive even when a nudge races the retry — a plain alarm counter
  cannot carry this, since any nudge resets it to zero. Tests:
  `workflow_steps_past_the_handler_budget_complete`,
  `workflow_nudge_racing_the_budget_retry_completes`. Docs:
  `docs/services/workflows.md`.
- `0126` (`d5f2f5b`) retry transient `OperationAborted` write conflicts (upstream #283).
  Transient bucket write conflicts (`OperationAborted`) failed the write at
  once. The bucket layer now retries them with backoff instead of surfacing a
  lost-race error; the retry matches the tagged service code in the error's
  `source`, not the object key (a key containing `OperationAborted` must not
  read as transient). Tests: `operation_aborted_is_not_a_lost_race` plus
  bucket unit tests over a conflicting fake store.
- `0127` (`4121b20`) disarm the restored alarm on failed start (upstream #276).
  A restored alarm whose start failed stayed armed and refired into the same
  failure. A failed start now disarms it — if the alarm is covered — in both
  the runtime and the logic layer (plus its probe); an uncovered restored
  alarm has no entry for the scan to rediscover, so the core re-arms its
  timer at scan pace (`FAILED_START_ALARM_REARM_MS = 60 s`) instead of
  dropping it. Tests: `a_failed_start_disarms_the_restored_alarm`.
- `0128` (`a581f5e`) bound per-statement execution time (upstream #274).
  A runaway SQL statement ran until the operation deadline. New knob
  `CELLD_SQL_STATEMENT_MS`: when set, each statement is interrupted past the
  bound and fails with a catchable timeout error naming the armed budget —
  the message names the captured budget, never a fresh knob read — that rolls
  the statement back; unset means unbounded, the historical behavior. A spent
  deadline rejects before stepping, and a write cursor that expires after its
  first row drops only the cursor: its write already ran, so the expiry
  error carries the timeout's prefix but never claims a rollback. Streaming
  cursors are bounded end to end, and a cursor shares its call's single
  deadline with the prefix run: one budget for the whole scan, since a second
  fresh budget at cursor creation would let a slow prefix plus a slow stream
  run up to ~2x the knob. A timeout flag left set by an armed run must not
  rewrite a later unarmed error: only a run that armed a deadline may report
  a timeout. The knob lives in the process environment, so the tests that
  flip it run serially. Tests: `long_statement_fails_catchable_and_rolls_back`,
  `streaming_cursor_is_bounded_end_to_end`,
  `unarmed_error_after_timeout_is_not_retimed`,
  `streaming_cursor_shares_the_calls_single_deadline`.
- `0129` (`bb4ebc0`) route dead-owner takeovers with fleet capacity (upstream #263).
  Dead-owner takeovers ignored fleet capacity, piling cells onto nodes with no
  room. Placement now routes with capacity: a zero-capacity node hands the dead
  owner's cell to a peer. Tests:
  `a_zero_capacity_node_hands_a_dead_owners_cell_to_a_peer`.
- `0130` (`79b5a9c`) share tenant roots across fetch, sockets, websockets (upstream #230).
  Tenant TLS trust differed per surface. `fetch()`, worker sockets, and outbound
  WebSockets now share one root set: Mozilla's anchors plus the OS store and
  `SSL_CERT_FILE` (via `rustls-native-certs`, cached). Native anchors a
  parser rejects are skipped before reqwest sees the bytes: under
  `rustls-tls`, one malformed anchor fails `ClientBuilder::build()` and
  breaks every tenant client, as the rustls clients skip what they cannot
  parse. A downloaded CA the OS trusts now works for tenant `fetch` too.
  Tests: `native_loader_honors_ssl_cert_file`,
  `root_store_holds_mozilla_anchors` (plus the `test-ca-230.pem` fixture),
  and the `tls-roots` black-box conformance: a loopback HTTPS server whose
  self-signed anchor is trusted only through `SSL_CERT_FILE`, fetched through
  the real tenant `fetch` client — with the variable set the fetch succeeds,
  without it the fetch fails.
- `0131` (`f3d54f7`) host-injected `CELLD_NODE` identity (upstream #219).
  `env` additionally carries `CELLD_NODE`: the name of the node running the
  code, injected by the host after every deployment binding. A bundle that must
  refuse a credential minted for another region reads this, not placement. The
  name is reserved — `vars`, `.dev.vars`, and every other binding kind are
  refused at deploy time, and a loaded Worker's props cannot smuggle it past
  the host value. This changes deploy behavior: projects binding the name now
  fail fast. A `CELLD_NODE` entry in `.env`/`.env.local` is skipped with a
  warning instead of refused (dotenv files serve other tools too; the host
  value wins at runtime regardless), while explicit `vars` and `.dev.vars`
  keep the hard refusal. Tests: `node-identity` fixture plus
  `node_identity_conformance` (including `node_identity_binding_name_is_reserved`
  and `node_identity_dotenv_entry_is_skipped`). Docs:
  `docs/cloudflare-compat.md`.
- `0132` (`668a051`) export `OTEL_RESOURCE_ATTRIBUTES` (upstream #314).
  The OTLP resource now parses the standard `OTEL_RESOURCE_ATTRIBUTES` env var
  into ordered resource entries. `service.name` in the extra entries is ignored
  (the configured service name wins); `service.version` in the extra entries names
  the application version, and celld's own build stays visible under
  `celld.version`, which cannot collide with an application key. Built-in
  resource keys win over colliding entries (OTLP requires unique attributes),
  and keys and values are percent-decoded before export. `service.name`
  precedence is env (`OTEL_SERVICE_NAME`), then resource attributes, then
  the `celld` default; docs scope these attributes to OTLP (the bucket
  Parquet files carry only `node` and `region`). Default behavior change:
  without an operator version there is no `service.version` at all —
  reporting celld's version there told every deployment-tracking backend
  that all applications run celld's build. New env knob
  `OTEL_RESOURCE_ATTRIBUTES` (default unset). Tests:
  `resource_attributes_parse_leniently`. Docs: `docs/telemetry.md`.
- `0133` (`0ad36af`) expose the handler traceparent (upstream #313).
  celld reads the W3C `traceparent` header on incoming requests and sends it
  on; a handler reads the traceparent of the span it runs in through
  `getCurrentTraceparent()` from `cloudflare:workers`, so it can return the
  value for end-to-end tracing. Tests: `trace-context` fixture plus
  `trace_context_conformance`. Docs: `docs/telemetry.md`.
- `0134` (`985a71e`) record incoming HTTP method and status (upstream #285).
  Incoming spans now carry the HTTP method and response status. The conformance
  re-issues calls with rewritten methods and, for the failure case, a masked
  status, so the export tells the worker spans (GET/200) apart from the DO's
  cell fetch spans (POST-or-PUT/500). Tests: `telemetry-http` fixture plus
  `telemetry_http_conformance`. Docs: `docs/telemetry.md`.
- `0135` (`4fbf738`) cap cells per isolate (upstream #278).
  New knob `CELLD_MAX_CELLS_PER_ISOLATE`, default 32 (unset restores the
  historical packing); heavyweight cells such as agents can lower the
  per-isolate packing. Tests: `max_cells_per_isolate_is_configurable`.
- `0136` (`a926ae5`) split log sink from traces (upstream #153).
  New knob `CELLD_OTEL_LOGS`: unset means logs follow the traces sink, the
  historical behavior; `bucket` sends logs to bucket Parquet whatever the
  traces sink is; any other value refuses startup. A fleet can point its
  collector at traces while its logs land in the bucket. Tests:
  `lookup_parses_the_logs_sink`, `split_flush_routes_each_signal_to_its_sink`.
  Docs: `docs/telemetry.md`.

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
- `v0.6.3-ryuzcorp.6` (`d15ce19`): adds
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
- `v0.6.3-ryuzcorp.7` (`45c0af0`): adds
  `0043`–`0050`. Upgrade notes:
  - `celld deploy` and `celld d1` refuse a project path that resolves
    outside the project directory, which is the config file's own
    directory (`0050`). That covers the config, `main`, assets,
    Dockerfiles and build contexts, `migrations_dir`, Python sources,
    and every file esbuild bundles. A monorepo that hoists
    `node_modules` above that directory no longer bundles: install the
    packages inside it. `migrations_dir` entries must be regular files,
    and a `containers[].image` that is an absolute or `..` path is
    refused.
  - On Linux, esbuild runs under Landlock with reads confined to the
    project, so `CELLD_ESBUILD` (or `esbuild` on `PATH`) must be the
    self-contained release binary, not a wrapper script (`0050`). A
    kernel or seccomp profile without Landlock runs it unconfined with
    a warning; Docker's, containerd's and podman's default profiles
    allow it.
  - `celld r2 head` and `celld r2 get --json` print a
    `{"error":"not_found",...}` object on stdout for an absent key
    (`0044`); the exit code and stderr are unchanged.
  - `celld --version` reports the full build version, such as
    `0.6.3-ryuzcorp.7` (`0046`).
  - New, opt-in: `CELLD_CACHE_SCOPE=deployment` shares one Workers
    Cache API scope across a deployment's scripts (`0045`; the default
    `script` scope is unchanged); `celld invoke` runs one request
    against a staged version before `celld activate` (`0048`), with
    `--header-env` for secret headers (`0049`); `celld deploy
    --sourcemap` collects a `no_bundle` entry's sibling map (`0047`).
  - `celld invoke` needs every node on this release: an older node has
    no `POST /invoke` route (`0048`). A stream received over
    RPC now stays open while the receiver holds it (`0043`), which ends
    the spurious aborts of signal-carrying container execs.
- `v0.6.3-ryuzcorp.8` (`dbf93e2`): adds `0051`.
  Upgrade notes:
  - `celld deploy --read-root DIR` (repeatable) names directories beyond
    the project that esbuild's inputs and `containers[].image`
    Dockerfiles with their build contexts may come from. A platform that
    deploys a built subdirectory of a checkout, or a monorepo app whose
    packages are hoisted above its config, names the checkout; with it,
    an absolute or `..` Dockerfile path inside the read root builds again.
    Without the flag, `.7`'s project-only bounds are unchanged.
- `v0.6.3-ryuzcorp.9` (`4b0b387`): adds `0052`–`0053`.
  Upgrade notes:
  - No behavior changes with `CELLD_DEPLOY_AWS_*` unset. A platform that
    sets the trio must give the deployment credential read access to both
    `deploy/` and `deploy-blobs/`, and keep the ordinary `AWS_*` grant
    read-write over the state prefixes (`cells/`, `log/`, `nodes/`,
    `fleet/`, `wake/`, `drain/`, `kv/`, `r2/`, `probe/`, `telemetry/`).
    The runtime also reads `wake-format.json` (GET) and lists `wake-v2/`
    and `wake-retired-v2/` (`wake_format.rs`), which one read-only prefix
    grant over `wake-` covers, and lists and deletes the `node-cells/`
    markers (`dead_node_gc.rs`), so that prefix stays read-write. The
    deploy command keeps the ordinary credential, which still needs write
    access to the deployment prefixes. A control-plane node, managed
    (`CELLD_CLOUD`) or BYO bucket alike, must not set the trio: it runs
    the deploy agent and publishes `deploy/` itself, so it refuses the
    trio at boot.
  - A serving node is non-dumpable: no core dump comes out of a node, and a
    non-root debugger or profiler cannot attach to one (`ptrace` is
    refused). Root tools are unaffected. Anything that reads the node's
    `/proc/<pid>/environ`, `mem`, or `maps` as the fleet uid now gets
    `EACCES` (`0053`). The window between `execve` and `main` setting the
    flag is still dumpable (initial boot, `celld dev`'s node respawn, the
    control plane's credential-rotation re-exec), and children a serving
    process spawns (`celld dev`'s esbuild, the presence agent's
    `hostname`) are dumpable again after `execve` but start with an
    emptied environment (`PATH`, `HOME`, `TMPDIR` only), so no credential
    reaches them. `celld dev`'s container engine CLI (projects with
    `containers`) still inherits the environment; a production node never
    builds.
- `v0.6.3-ryuzcorp.10` (`895a05d`): adds
  `0054`–`0057`, all bucket-request and storage savings; no format or API
  change, so nodes on `.9` and `.10` can share a bucket.
  Upgrade notes:
  - A bucket-posture eviction makes fewer requests (`0054`). An eviction
    that publishes a handoff snapshot then deletes, in the background, the
    per-cell L0/L1 objects the snapshot supersedes, one single-key
    `DeleteObject` per key (`0057`).
    The bucket credential therefore needs `s3:DeleteObject` on `cells/`,
    which the read-write state grant already covers; a failed delete only
    leaves garbage behind and never fails the eviction.
  - Concurrent bucket-posture writes on one cell share ownership reads
    (`0056`): fewer `cells/<cell>/own.json` GETs on hot cells at the same
    acknowledgement rule. The probes `GateProvenByBucket` and
    `GateOwnershipRefused` now count once per read, not per write.
  - A fleet-posture node with no peer it could ever recruit backs its
    `nodes/` scan off to the 30 s maintenance tick (`0055`), so a node that
    starts next to it is recruited within ~30 s instead of ~1 s (the new
    node recruits the old one itself at once).
- `v0.6.3-ryuzcorp.11` (`eac4160`): adds
  `0058`–`0067`, `0069`–`0072` and `0075`. `0068`, `0073` and `0074` were
  parked unmerged. There is no API or CLI change. A `.11` node publishes
  bucket format 3, which `.10` nodes read, so `.10` and `.11` nodes can
  share a bucket.
  Upgrade notes:
  - Bucket-posture group commit (`0059`–`0064`, `0070`, `0071`) is in the
    binary but off by default (`0075`); `CELLD_BUCKET_BUNDLES=1` turns it
    on. It needs every live lease to read format 3 (every node on `.11`)
    and falls back to per-cell uploads at once otherwise.
    - Measured on 200 agents × 6 writes with `DELAY_MS=100`: Class A
      requests fell from 1934 to 1140, but write-ack p50 rose from 210 to
      445 ms. On a hot cell, p50 rose from 235 to 432 ms. Enable it only
      where request cost matters more than write latency.
    - `.10` recovers a bucket that `.11` wrote bundles into (whole-prefix
      fold, no loss declared), so a downgrade is safe.
  - An eviction proves a journaled epoch with one HEAD instead of a prefix
    LIST (`0061`).
  - The C heap is trimmed sooner after evictions (`0065`, `CELLD_C_TRIM_MB`,
    default 8). Post-drain RSS fell by about 10 MiB on the agent-50 shape.
  - `CELLD_SQLITE_CACHE_KB` caps the per-connection SQLite page cache
    (`0066`; the default of 2000 KiB is unchanged).
  - A deployment generation projects its `/bundle` tree once instead of
    once per isolate (`0067`).
  - `tools/s3bench` (`0058`, `0069`, `0072`) measures bucket request counts
    and per-write latency against a local S3 for cost claims.
- `v0.6.3-ryuzcorp.12` (`00df62d`): adds `0076`–`0078`.
  There is no API, CLI or bucket format change, so `.11` and `.12` nodes can
  share a bucket, and a downgrade restores everything `.12` wrote.
  Upgrade notes:
  - An eviction skips the full-database handoff snapshot when the cell's
    restorable chain was born in this epoch, every acked row is already
    per-cell, and the chain holds at most 16 objects (`0078`, modelled on
    ClickHouse MergeTree's lazy merges). The successor restores the short
    L0 chain instead. A bounded number of extra Class B GETs replaces one
    Class A PUT and a full-database upload per eviction.
    - Measured on 200 agents × 6 writes, bucket posture, `DELAY_MS=100`,
      release builds: Class A 1934 → 1734 (−10%), PUT 1400 → 1200, and write
      p50 unchanged at 210 ms. On 50 agents: Class A 582 → 532.
    - Cells restored from an older epoch, cells with group-commit rows not
      yet per-cell, and longer chains still publish the snapshot as before.
  - `tools/s3bench` defaults to the release binary and refuses to start
    while another run is up (`0076`).
  - The integration tests boot nodes on ephemeral ports, which removes a
    port-reuse flake (`0077`).
- `v0.6.3-ryuzcorp.13` (the commit that adds this entry): adds `0079`–`0136`.
  New CLI: `celld versions [SCRIPT]` lists deployed versions (`0123`), `celld
  d1 --local` runs execute, export, and migrations against the running `celld
  dev` (`0121`), and `celld dev --state DIR` (or `CELLD_DEV_STATE`) keeps local
  state outside the project (`0124`). No bucket format bump and no wire
  protocol change between nodes, so `.12` and `.13` nodes can share a bucket;
  the only new bucket-side artifact is the `log/<node>.loss.json` loss record
  (`0094`), which older nodes never write but read past, and a downgrade
  restores everything `.13` wrote. Default behavior changes: the node-session
  id is pinned in the data dir (`0092`), `service.version` now carries the
  operator's application version instead of celld's build (`0132`), and
  WebSocket close codes 1001–1014 (other than the reserved no-status codes)
  now close instead of throwing (`0113`).
  Upgrade notes:
  - Runtime and cancellation: cancelled requests settle shared work instead of
    hanging co-awaiters (`0079`); answered drivers keep polling while waitUntil
    background is outstanding (`0081`); drivers take the mailbox once more
    before retiring and refused handoffs reject (`0083`); cells per isolate
    are capped by `CELLD_MAX_CELLS_PER_ISOLATE` (default 32) (`0135`);
    post-response background work ends past `CELLD_BACKGROUND_BUDGET_S`
    (default 30 s) without failing the answered request (`0101`); a cell
    whose SQLite transaction was destroyed restarts on a fresh isolate
    (`0104`); `AbortSignal.timeout` frees its cell slot (`0106`); loader
    cancellation publishes the abort synchronously (`0118`); the
    budget-orphaned workflow drive drops when a retry arrives (`0125`);
    covered restored alarms disarm on failed start while uncovered ones
    re-arm at scan pace (`0127`).
  - Fleet and durability: stale-target 409s prove a member is gone (`0088`);
    renewal readbacks are bounded to the remaining authority (`0089`); the
    first alarm-deadline expiry only marks the firing (`0090`); follower tails
    stream under a 10 s progress timeout (`0091`); same-guard CAS conflicts
    retry twice before the readback (`0093`); degraded same-member ensembles
    resume without an epoch step up to 3 consecutive resumes (`0095`);
    transient `OperationAborted` bucket writes retry (`0126`); dead-owner
    takeovers route with fleet capacity (`0129`); record-less session fragments
    are collected (`0103`); deleted sessions with surviving objects leave a loss
    record (`0094`); wake sleeps until the next upcoming alarm minute (`0096`).
  - Storage: local L0/epoch files survive compaction (`0098`); unclean
    restarts prune live-named epochs (`0105`); the dev store commits through
    one writer thread (`0099`); local LISTs read from a covering index
    (`0122`); the 0.5.1 facet-image import is repaired (`0100`); per-statement
    SQL time is bounded by `CELLD_SQL_STATEMENT_MS` (unset means unbounded)
    (`0128`); hydration counting is O(1) (`0102`).
  - Workers compatibility: `process.env` populates from worker vars under
    `nodejs_compat` (`0109`, with a `nodejs_compat_do_not_populate_process_env`
    opt-out); AES-KW wrap/unwrap plus symmetric JWK
    import/export (`0110`); EC public keys import and export as raw points
    (`0111`); writable request `/tmp` files and `node:url` file
    helpers (`0112`, capped per request by `CELLD_TMP_MAX_BYTES`, default
    64 MiB); gzip/deflate response bodies decompress transparently (`0114`);
    `Symbol.dispose` attaches to plain object and array RPC results (`0115`);
    `dir/index.html` serves at its trailing-slash URL (`0116`); post-response
    DO dials route by the object's cell (`0117`); external stubs union main
    and sibling imports (`0108`); the 101 webSocket target crosses the loader
    fetch (`0107`);
    tenant TLS roots are shared across fetch, sockets, and websockets
    (`0130`); close codes accept workerd's sendable range (`0113`).
  - CLI and deploy: `celld versions` (`0123`); `d1 --local` with `--state DIR`
    selecting the local state dir (`0121`); `dev --state` / `CELLD_DEV_STATE`
    (`0124`); image tars upload multipart in 8 MiB parts (`0120`); durable
    object bindings with a foreign `script_name` are refused at deploy time
    (`0119`); `CELLD_NODE` is host-injected and its binding name is reserved
    (`0131`).
  - Telemetry: `OTEL_RESOURCE_ATTRIBUTES` feeds the OTLP resource, with
    `service.version` now operator-owned and the build under `celld.version`
    (`0132`); the handler traceparent is exposed through
    `getCurrentTraceparent()` (`0133`); incoming spans carry HTTP method and
    status (`0134`); `CELLD_OTEL_LOGS=bucket` splits the log sink from traces
    (`0136`).
  - New env knobs and their defaults: `CELLD_BACKGROUND_BUDGET_S` (default 30),
    `CELLD_MAX_CELLS_PER_ISOLATE` (default 32), `CELLD_SQL_STATEMENT_MS`
    (unset means unbounded), `CELLD_TMP_MAX_BYTES` (default 67108864, 64 MiB),
    `CELLD_OTEL_LOGS` (unset means logs follow
    traces), `OTEL_RESOURCE_ATTRIBUTES` (unset), `CELLD_DEV_STATE` (default
    `PROJECT/.celld/dev`), `CELLD_NODE` (default generated and pinned to
    `node-id` in the data dir).

## License

Apache-2.0, like upstream. `NOTICE` is kept, and every file the patches
modify carries a `Modified by ryuzcorp (ryuzcorp/celld)` notice naming
what changed, per Apache-2.0 §4(b).
