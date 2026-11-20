---
title: Maintainers and contributing
description: How Cello is branched, reviewed, and released.
---

Cello is agent-run: every change lands through a pull request into
`main` (rebase merges only), required checks stay green on both
architectures, and an independent review — explicitly diffing
`.github/` and `Dockerfile` — records its verdict before merge.
`upstream/main` mirrors upstream denoland/celld and carries no patches; upstream
fixes are ported by hand. Releases are cut by pushing `v<semver>` tags;
tags are never moved or reused.

The full process, including what goes where and the license notices,
lives in
[`CONTRIBUTING.md`](https://github.com/ryuzcorp/cello/blob/main/CONTRIBUTING.md)
at the repo root.
