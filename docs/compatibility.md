---
title: Compatibility with celld
description: What Cello keeps byte-identical with celld, and how to upgrade a celld deployment.
---

Cello 0.1.0 is the renamed continuation of celld
`v0.6.3-ryuzcorp.13`: the same code, the same on-disk and on-bucket
formats, new names. A bucket written by `v0.6.3-ryuzcorp.13` keeps
working under Cello 0.1.0 with no migration step.

## Frozen identifiers

These `celld`-spelled names stay exactly as they are, because they are
persisted to bucket objects, cell SQLite, or stored values — or they are
exchanged node-to-node on the wire, where a mixed fleet must keep
gossiping while it upgrades. Everything else named `celld` becomes
`cello`, with no fallbacks.

Persisted (a `.13` bucket opens under 0.1.0 because the reader side is
unchanged):

- `__celld-runtime` — the script name of the runtime isolate pool that
  hosts `__D1Database`, `__KvNamespace`, and `__Queue`. Recorded in
  stored deployment graphs and used in cell-to-script resolution.
- The stored-stub envelope family — `__celld$svc`, `__celld$stub`,
  `__celld$req`, `__celld$res`, `__celld$hdr`, `__celld$blob`,
  `__celld$rs`, `__celld$ws`, `__celld$sig`, `__celld$disp`,
  `__celld$loaderSvc`, and the `__celld$` prefix guard itself. A
  stub-bearing row in cell SQLite carries these keys; revive looks them
  up by exact name. Rows written before the binding-isolation patches
  still decode as plain data on first read; storing them again writes
  the new envelope.
- Isolate↔host dunder tokens — the `__celld` JS global, the `__cell.*`
  namespace, `__make*` ops, `__celldBodyBytes`, `__celldMaterial`,
  `__celldHeaderList`, `__celldDigestState`, `__celldStreamId`,
  `__celldCancelId`, `__celldRpcSignalSubscription`, the
  `__CELLD_ACTOR_ABORT__:` / `__CELLD_PROCESS_EXIT__:` /
  `__CELLD_DO_ROUTING_ERROR__:` sentinels, and the test-only
  `__celldStubStatsForTestOnly`, `__celldPlantStoredV1ForTestOnly`, and
  `__CELLD_TEST_WORKFLOW_*` markers. Producer and consumer must agree
  exactly, so they stay byte-identical.
- The R2 user-metadata envelope `celld-r2` (`celld_r2` on Azure Blob
  Storage, whose backend translates the hyphen), with the cursor family
  `celld-r2-cursor-` / `celld-r2-cursor-v1.`. The writer and reader are
  the same code, so no dual-read is needed.
- The telemetry object metadata `celld-schema` (and `celld-retention`)
  on Parquet batch objects in the bucket, so batches written by `.13`
  stay readable. The OTLP resource attributes rename —
  `celld.version` becomes `cello.version` — because those are emitted
  fresh on every export, never read back.

Persisted container references:

- the `celld-image:` prefix recorded in stored `.13` deployment
  manifests (`ContainerSpec.image`), resolved to
  `deploy/images/<key>.tar`, so an old manifest's image reference
  still resolves to its tar.

Wire (node-to-node, unchanged so mixed versions gossip while you roll):

- the `celld-tunnel` WebSocket subprotocol on `/peer/tunnel`,
- the `x-celld-overload` overload signal on peer RPC responses,
- the `celld-log-stream` upgrade value for peer log-tail streams,
- the overload marker value
  `celld-internal-cell-overload-7ec38c64-12d7-4ddc-9e77-b63f9dc14130`,
  which crosses nodes inside peer-forwarded errors.

Upstream `denoland/celld#NNN` links in comments and docs are historical
references and still resolve. Fence image names such as `celld-fence…`
recorded opaquely in existing deployment manifests keep working because
the name passes through to the container runtime, never parsed; new
deployments record `cello-fence…`.

## Upgrading from a celld deployment

1. **Environment.** Every `CELLD_*` variable becomes `CELLO_*`, strictly
   one-to-one, with no fallbacks and no aliases: `CELLD_BUCKET` →
   `CELLO_BUCKET`, `CELLD_OPERATOR_TOKEN` → `CELLO_OPERATOR_TOKEN`,
   `CELLD_DEV_STATE` → `CELLO_DEV_STATE`,
   `CELLD_DEPLOY_AWS_*` → `CELLO_DEPLOY_AWS_*`, and so on for all 220
   names. The node, `cello dev`, and every CLI refuse to start when any
   `CELLD_*` variable is set, naming the `CELLO_*` spelling — a
   carried-over environment can never silently lose config.
2. **Image.** `ghcr.io/ryuzcorp/celld:<version>-ryuzcorp.<n>` becomes
   `ghcr.io/ryuzcorp/cello:<semver>` (`0.1.0`, `0.1`, `beta`,
   `latest`). Consumers pin by digest as before.
3. **Local state.** `cello dev` keeps its state in `.cello/dev`
   instead of `.celld/dev` (same `--state DIR` / `CELLO_DEV_STATE`
   override). The old directory is not migrated and cannot be opened:
   `cello dev` refuses an explicit `--state` directory it did not
   create, and a `.13` state directory carries no marker — so keep it
   aside for reference and start clean state for the renamed binary.
4. **Bucket and wire.** No migration step: the fleet bucket, its
   prefixes, and the node-to-node protocol are unchanged, so a `.13`
   bucket opens under 0.1.0 directly. The only new bucket-side artifact
   is the `log/<node>.loss.json` loss record, which older nodes never
   write but read past.
5. **Health and telemetry.** The public health path moves to
   `/.well-known/cello/health`; update liveness probes. Telemetry
   metric and resource names move from `celld.*` to `cello.*`
   (`cello.version` reports the build); update dashboards and alerts.
6. **Roll together past isolation changes.** If the deployment predates
   `v0.6.3-ryuzcorp.6`, read the `.6` upgrade notes in the
   [Changelog](changelog.md) first: binding isolation, socket ownership,
   and the operator token all fail closed across mixed versions, so
   upgrade every node of the fleet together.
