---
title: Upgrade Cello
description: Move between releases without data loss, choosing a rolling or stop-all upgrade for your path.
---

You upgrade by swapping the binary or image on the same bucket. Some releases roll node by node; others need every node stopped first. Check your path below before you start.

## Pick the upgrade path

- **Stop all:** stop every node, then start the new version on the same bucket. No export, no import, no rewrite pass. Old data stays as it is until it is naturally rewritten.
- **Rolling:** stop each node with SIGTERM, wait for its replacement to report healthy, then move to the next node. See [shut down and roll out](run-fleet.md#shut-down-and-roll-out-a-node).

Never run a mixed fleet except during a rolling upgrade, and never downgrade past the limit your path names.

| Path | Mode | Downgrade limit |
| --- | --- | --- |
| celld / Cello 0.1.x to v0.2.0 | Stop all | No mixed fleet: 0.2.0 speaks a new peer protocol |
| v0.2.1 to v0.3.0 | Rolling | Do not start v0.2.x after a node runs v0.3.0 unless its shutdown log contains `node-log close: sealed epoch`, or you can lose acknowledged writes. The default durability changes from `bucket` to `fleet` |
| v0.3.0 to v0.4.0 | Stop all | A v0.3.0 node cannot read new large KV values, so a mixed fleet can make committed KV values unavailable |
| v0.4.0 to v0.4.1 | Rolling | Paged restore starts one lease lifetime after the last v0.4.0 node stops. Do not start v0.4.0 after that: it cannot restore a paged cell |
| v0.5.1 to v0.6.0 | Stop all with `fleet` durability | A v0.6.0 node refuses to start against a v0.5.1 follower. A `bucket` durability fleet can roll |
| v0.6.0 to v0.6.1 | Rolling | Until every node runs v0.6.1, do not set `CELLO_LTX_RETENTION_SECS`, do not deploy a Python Worker, and do not raise `CELLO_MAX_ASSET_FILE_BYTES` above 25 MiB. A rollback to v0.6.0 can roll after you unset `CELLO_LTX_RETENTION_SECS` on every node |
| v0.6.1 to v0.6.2 | Rolling | Until every node runs v0.6.2, a handler failure on the owner of a forwarded Durable Object fetch can reach the caller as a 500 response. A rollback to v0.6.1 can also roll |
| v0.6.2 to v0.6.3 | Rolling | Upgrade each node promptly: v0.6.3 no longer trusts the emSign ECC Root CA - G3 certificate or eight compromised eMudhra intermediate CA certificates for outbound TLS. Until every node runs v0.6.3, a v0.6.2 node can still acquire a cell whose ownership record was lost and start it with an empty database. A rollback to v0.6.2 can also roll |

## Upgrade from celld or 0.1.x to 0.2.0

1. **Stop all old nodes.** Cello 0.2.0 speaks a new peer protocol (`cello-tunnel`, `x-cello-overload`, `cello-log-stream`, and a new overload marker), so an old node and a new node cannot share a fleet.
2. **Swap the binary or image.** `ghcr.io/ryuzcorp/cello:<semver>` (`0.2.0`, `0.2`, `beta`, `latest`) replaces `ghcr.io/ryuzcorp/celld:<version>-ryuzcorp.<n>`. Consumers pin by digest as before. The bucket, its prefixes, and the node data directories stay where they are.
3. **Rename environment variables.** Every `CELLD_*` variable becomes `CELLO_*`, strictly one-to-one, with no fallbacks and no aliases: `CELLD_BUCKET` → `CELLO_BUCKET`, `CELLD_OPERATOR_TOKEN` → `CELLO_OPERATOR_TOKEN`, `CELLD_DEV_STATE` → `CELLO_DEV_STATE`, and so on. The node, `cello dev`, and every CLI refuse to start when any `CELLD_*` variable is set, naming the `CELLO_*` spelling. A stale `CELLD_OPERATOR_TOKEN` never leaves the internal listener open: the node fails closed before serving anything.
4. **Start Cello on the same bucket.** Cello 0.2.0 reads data a celld node wrote (new writes use the `cello` spellings).

Persisted identifiers are written with the `cello` spelling and read with both, in one place per family: the isolate pool script (`__cello-runtime`, still resolving `__celld-runtime`), stored stub rows, the R2 user-metadata envelope (`cello-r2`, and `cello_r2` on Azure; cursors under both `cello-r2-cursor-v1.` and `celld-r2-cursor-v1.`), telemetry batch stamps (`cello-schema` / `cello-retention` beside `celld-schema` / `celld-retention`), and container image references (`cello-image:<key>` beside `celld-image:<key>`). New writes use the new spelling.

What does not carry over: the wire protocol (mixed fleets are unsupported), in-process names (the `__cello` global and `__CELLO_*__` sentinels were renamed outright), the old `.celld/dev` local state directory, and `celld.*` health and telemetry names (already `cello.*` since 0.1.0 — update dashboards and liveness probes).
