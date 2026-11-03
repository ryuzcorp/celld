# ryuzcorp/celld

This is ryuzcorp's fork of [denoland/celld](https://github.com/denoland/celld).
Upstream ships only security fixes from 0.6.x on, so this fork carries the
feature patches we need on top of the pinned upstream releases. Upstream's
README below is kept intact; this file documents the fork.

## Branches

- `main` mirrors upstream `main` and carries no patches.
- `ryuzcorp/v<version>` is a line branch: the upstream tag `v<version>`
  plus our patches as commits, one commit per patch, in `0001`-`0006`
  order. Each patch commit is self-contained and carries its own tests.

Upstream's `.github/workflows/release.yml` targets `ghcr.io/denoland` and
an upstream candidate branch, so the line branches replace it with our
own `ci.yml` (build and test both architectures, push nothing) and
`publish.yml` (publish release tags); `main` keeps upstream's file.

## Tags and images

An annotated tag `v<version>-ryuzcorp.<n>` on the matching line branch is
a release: `publish.yml` verifies the tagged commit is contained in
`ryuzcorp/v<version>`, refuses to overwrite an already-published level,
and publishes the multi-arch image

    ghcr.io/ryuzcorp/celld:<version>-ryuzcorp.<n>

with labels `org.opencontainers.image.source`,
`org.opencontainers.image.version`, `org.opencontainers.image.revision`
(the tagged commit), and `now.ryuzcorp.celld.upstream` (the upstream
tag's commit). Consumers pin the image by digest.

Images published before the `-ryuzcorp` rename carry the legacy tag
names `0.6.3-noite.1..3`; they are the same commits as `ryuzcorp.1..3`.

## Adding a patch

Open a pull request into the line branch: one commit, self-contained,
with tests. `ci.yml` must be green and the owner reviews every patch
before it lands.

## Cutting a release

Tag the line branch and push the tag; `publish.yml` does the rest:

    git tag -a v<version>-ryuzcorp.<n> <commit> -m '...'
    git push origin v<version>-ryuzcorp.<n>

The tag must point at a commit on `ryuzcorp/v<version>`, and `<version>`
must match the tree's `crates/celld/Cargo.toml`; a published level is
never rebuilt, so cut a new `<n>` for every change.

## Moving to a new upstream release

Start a new line branch at the new upstream tag and rebase the patch
commits onto it:

    git checkout -b ryuzcorp/v<new> v<new>
    git rebase --onto v<new> v<old> ryuzcorp/v<old>

then resolve conflicts, run the full test suite, and open a pull request
into the new line branch.

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

## License

Apache-2.0, like upstream. `NOTICE` is kept, and every file the patches
modify carries a `Modified by ryuzcorp (ryuzcorp/celld)` notice naming
what changed, per Apache-2.0 §4(b).
