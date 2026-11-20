---
title: Telemetry
description: OTLP resource attributes, traceparent exposure, span method and status, and split log sink.
---

Patches to what Cello records and where it sends it. The
[Telemetry](../../guides/telemetry.md) page is the reference; these are the
deltas over upstream denoland/celld. Each entry names its fork patch number.

- `0132` — The OTLP resource parses the standard
  `OTEL_RESOURCE_ATTRIBUTES` env var into ordered resource entries.
  `service.name` in the extra entries is ignored (the configured
  service name wins); `service.version` in the extra entries names the
  application version, and Cello's own build stays visible under
  `cello.version`, which cannot collide with an application key.
  Built-in resource keys win over colliding entries, and keys and
  values are percent-decoded before export. `service.name` precedence
  is env (`OTEL_SERVICE_NAME`), then resource attributes, then the
  `cello` default. Default behavior change: without an operator
  version there is no `service.version` at all — reporting Cello's
  version there told every deployment-tracking backend that all
  applications run Cello's build
  ([upstream #314](https://github.com/denoland/celld/issues/314)).
- `0133` — The handler reads the traceparent of the span it runs in
  through `getCurrentTraceparent()` from `cloudflare:workers`, so it
  can return the value for end-to-end tracing
  ([upstream #313](https://github.com/denoland/celld/issues/313)).
- `0134` — Incoming spans carry the HTTP method and the response
  status, so worker spans and cell-fetch spans tell apart in export
  ([upstream #285](https://github.com/denoland/celld/issues/285)).
- `0136` — `CELLO_OTEL_LOGS` splits the log sink from traces: unset
  means logs follow the traces sink (the historical behavior);
  `bucket` sends logs to bucket Parquet whatever the traces sink is;
  `0` drops logs while traces keep exporting; any other value refuses
  startup. A fleet can point its collector at traces while its logs
  land in the bucket
  ([upstream #153](https://github.com/denoland/celld/issues/153)).
