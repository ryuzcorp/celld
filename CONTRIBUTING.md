# Contributing to Cello

Cello is run by agents: every change lands through a pull request, and
every PR gets an independent review before merge. There is no human
backstop, so the process is strict on purpose.

## Branching

- PRs target `main`. The repo allows rebase merges only; keep history
  linear and rebase instead of merging `main` into your branch.
- `upstream/main` mirrors upstream denoland/celld and carries no patches. Fixes
  from upstream are ported by hand, one commit per fix, never merged.
- Releases are cut from tags: pushing `v<semver>` runs the release
  pipeline (build, provenance, image publish). The tag must be
  annotated, name the version in `crates/cello/Cargo.toml` exactly, and
  point at a commit on `main` — anything else is refused before the
  build. Never reuse a version, never move a tag.
- Direct pushes, force-pushes, and deletions on protected branches are
  blocked by ruleset.

## Required checks

A PR merges only when its required checks are green — currently the
`test` job (full suite plus clippy in the Dockerfile test stage) on
both architectures — and an independent review is recorded, with every
High and Medium finding fixed and re-verified before merge. The review
must explicitly diff `.github/` and `Dockerfile` on every PR: a PR runs
its own CI, so a patch that neuters its own tests still shows green
checks.

## What to change where

- Rust, Docker, CI, tools, examples: this repo, same PR as its tests.
- Docs: `docs/` is the Blume content folder; see
  [Maintainers](https://cello.ryuz.eu/contributing). Build with
  `npm run dev`, gate with `npm run validate`.
- One concern per PR, one commit per patch, with tests. Delete
  throwaway scripts before requesting review.

## License

Apache-2.0. Modified files keep their `Modified by Ryuz Corp (Cello)`
notice, per Apache-2.0 §4(b); `NOTICE` keeps every existing
third-party notice.
