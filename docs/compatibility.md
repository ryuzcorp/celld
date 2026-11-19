---
title: Upgrading to 0.2.0
description: Swap the binary or image on the same bucket; what Cello reads, writes, and no longer speaks.
---

Cello 0.2.0 keeps your data and changes the wire. Stop every node of the
old fleet, then start Cello 0.2.0 on the same bucket: celld-written data
carries over, new writes use the `cello` spellings, and a mixed fleet of
old and new nodes is unsupported.

## Upgrade steps

1. **Stop all old nodes.** Cello 0.2.0 speaks a new peer protocol
   (`cello-tunnel`, `x-cello-overload`, `cello-log-stream`, and a new
   overload marker), so an old node and a new node cannot share a fleet.
   There is no rolling upgrade from [denoland/celld](https://github.com/denoland/celld)
   or Cello 0.1.x: stop every node first, then start 0.2.0.
2. **Swap the binary or image.** `ghcr.io/ryuzcorp/cello:<semver>`
   (`0.2.0`, `0.2`, `beta`, `latest`) replaces
   `ghcr.io/ryuzcorp/celld:<version>-ryuzcorp.<n>`. Consumers pin by
   digest as before. The bucket, its prefixes, and the node data
   directories stay where they are.
3. **Rename environment variables.** Every `CELLD_*` variable becomes
   `CELLO_*`, strictly one-to-one, with no fallbacks and no aliases:
   `CELLD_BUCKET` → `CELLO_BUCKET`, `CELLD_OPERATOR_TOKEN` →
   `CELLO_OPERATOR_TOKEN`, `CELLD_DEV_STATE` → `CELLO_DEV_STATE`, and so
   on. The node, `cello dev`, and every CLI refuse to start when any
   `CELLD_*` variable is set, naming the `CELLO_*` spelling — a carried
   over environment can never silently lose config. In particular a stale
   `CELLD_OPERATOR_TOKEN` never leaves the internal listener open: the
   node fails closed before serving anything.
4. **Start Cello on the same bucket.** No export, no import, no rewrite
   pass. Old data stays as it is until it is naturally rewritten.

## What carries over

Persisted identifiers are written with the `cello` spelling and read
with both, in one place per family, so the old spelling is obvious and
easy to delete later:

- The runtime isolate pool script: new graphs record `__cello-runtime`;
  graphs that record `__celld-runtime` still resolve to the runtime pool.
- Stored stub rows: rows written before the binding-isolation patches
  still decode as plain data on first read; storing them again writes
  the new envelope.
- The R2 user-metadata envelope: new objects carry `cello-r2`
  (`cello_r2` on Azure Blob Storage); objects carrying `celld-r2` read
  the same way. Cursors work under both the `cello-r2-cursor-v1.` and
  the `celld-r2-cursor-v1.` spellings; new cursors use the new one.
- Telemetry batches: the retention sweep lists the date-partitioned key
  and never reads the forensic stamp, so batches stamped `celld-schema`
  / `celld-retention` keep their lifecycle. New batches are stamped
  `cello-schema` / `cello-retention`. Readers of the stamp itself must
  accept both spellings.
- Container image references: manifests that record `celld-image:<key>`
  resolve to the same `deploy/images/<key>.tar` as `cello-image:<key>`;
  new manifests record the new spelling.

Upstream `denoland/celld#NNN` links in comments and docs are historical
references and still resolve.

## What does not carry over

- **The wire.** The subprotocol, the overload signal, the log-stream
  upgrade value, and the overload marker all use new spellings. Mixed
  fleets are unsupported: a 0.2.0 node never peers with an older one.
- **In-process names.** The isolate↔host `__cello` global, the
  `__cello*` tokens, the `__CELLO_*__` sentinels, and the test-only
  markers are never persisted; they were renamed outright.
- **Local state.** `cello dev` keeps its state in `.cello/dev`; the old
  `.celld/dev` directory is not read.
- **Health and telemetry names** moved from `celld.*` to `cello.*` in
  0.1.0 already (`cello.version` reports the build); update dashboards
  and liveness probes if you have not.
