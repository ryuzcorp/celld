---
title: What Cello adds over upstream
description: Every improvement Cello carries over upstream denoland/celld, grouped by area.
---

Cello is upstream denoland/celld `v0.6.3` plus 136 patches, carried
one-commit-per-patch and released as `v0.6.3-ryuzcorp.4` through `.13`
before the rename to Cello 0.1.0 (0.2.0 keeps the same patch stack and
renames the frozen identifiers — see [Compatibility](../compatibility.md)).
This section turns those patch entries into readable
groups. Each entry names its fork patch number; `(upstream #NNN)` links
are the upstream issues a patch fixes or implements.

- [Runtime](runtime.md) — JavaScript runtime, RPC, container exec,
  isolate concurrency, and cancellation.
- [Fleet and durability](fleet-durability.md) — ownership, takeover,
  staged activation, and bucket-posture group commit.
- [Storage](storage.md) — eviction, dev-store throughput, repair, and
  SQL bounds.
- [Workers compatibility](workers-compat.md) — cache, containers,
  crypto, SQL, and runtime APIs closer to Cloudflare.
- [CLI](cli.md) — new `cello` commands and flags.
- [Efficiency and cost](efficiency.md) — fewer bucket requests, less
  memory, and the s3bench harness.
- [Security and hardening](security-hardening.md) — binding isolation,
  operator token, egress policy, confined deploys, credential hygiene.
- [Telemetry](telemetry.md) — OTLP attributes, traceparent, span
  method/status, and split log sink.

Patches `0025`–`0034` are ported fixes from
[meln1k/celld](https://github.com/meln1k/celld) (Apache-2.0, same
author line kept with a `Ported-from` trailer). Patch numbers `0068`,
`0073`, and `0074` were parked unmerged and do not exist.

Settings and defaults are stated on each page. The per-release
rollup — what shipped together and what mixed versions tolerate — is
the [Changelog](../changelog.md).
