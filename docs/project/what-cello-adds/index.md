---
title: What Cello adds over upstream
description: Every improvement Cello carries over upstream denoland/celld, grouped by area.
---

Cello is upstream denoland/celld `v0.6.3` plus 136 patches, carried one-commit-per-patch. Each page below turns one patch group into readable notes. Every entry names its fork patch number; `(upstream #NNN)` links are the upstream issues a patch fixes or implements.

- [Runtime](runtime.md) — fetch and RPC semantics, container exec, isolate sharing, and cancellation.
- [Fleet and durability](fleet-durability.md) — ownership, takeover, staged activation, and bucket-posture group commit.
- [Storage](storage.md) — eviction, dev-store throughput, repair, and SQL bounds.
- [Workers compatibility](workers-compat.md) — cache, containers, crypto, SQL, and runtime APIs closer to Cloudflare.
- [CLI](cli.md) — new `cello` commands and flags.
- [Efficiency and cost](efficiency.md) — fewer bucket requests, less memory, and the s3bench harness.
- [Security and hardening](security-hardening.md) — binding isolation, operator token, egress policy, and confined deploys.
- [Telemetry](telemetry.md) — OTLP attributes, traceparent, span shape, and split log sink.

Patches `0025`–`0034` are ported fixes from [meln1k/celld](https://github.com/meln1k/celld) (Apache-2.0, same author line kept with a `Ported-from` trailer). Patch numbers `0068`, `0073`, and `0074` were parked unmerged and do not exist.

Settings and defaults are stated on each page. The per-release rollup — what shipped together and what mixed versions tolerate — is the [Changelog](../../changelog.md).
