# ryuzcorp/celld

This is ryuzcorp's fork of [denoland/celld](https://github.com/denoland/celld).
Upstream ships only security fixes from 0.6.x on, so this fork carries the
feature patches we need on top of the pinned upstream releases. Upstream's
README below is kept intact; this file documents the fork.

## Branches

- `main` mirrors upstream `main` and carries no patches.
- `ryuzcorp/v<version>` is a line branch: the upstream tag `v<version>`
  plus our patches as commits, one commit per patch, in `0001`-`0015`
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
- `0015` (sha TBD at merge) container exec kill reaches the process:
  the kill op runs under the start's lifetime (a fire-and-forget kill
  no longer dies with its event), and the signal is delivered by an
  in-container scan for the exec's marker env instead of a host-pid
  `kill` (the daemon reports host PIDs, and images may lack a `kill`
  binary). A killed exec's `exitCode` settles with its signal exit,
  and the container is untouched. The image needs `sh`, `grep` and
  `/proc`; an undelivered kill logs `container_exec_kill_failed`. This
  also makes `0010`'s abort signal reach the process.

## Releases

- `v0.6.3-ryuzcorp.4` (`65419c6`): patches `0001`–`0006`; first
  release published by this fork.
- `v0.6.3-ryuzcorp.5` (`750e18a`): adds `0007`–`0008` (PR #2).

## License

Apache-2.0, like upstream. `NOTICE` is kept, and every file the patches
modify carries a `Modified by ryuzcorp (ryuzcorp/celld)` notice naming
what changed, per Apache-2.0 §4(b).
