---
title: Telemetry
description: OTLP resource attributes, traceparent exposure, span method and status, and split log sink.
---

Cello records the same traces and logs as upstream, but it tags them so your backend can route them. The [Telemetry](../../guides/telemetry.md) guide is the reference for what to set; these are the deltas over upstream denoland/celld. Each entry names its fork patch number.

## Route exports with standard attributes

- `0132` — The OTLP exporter parses the standard `OTEL_RESOURCE_ATTRIBUTES` env var into ordered resource entries. `service.name` in the extra entries is ignored (the configured service name wins); `service.version` names your application version, and Cello's own build stays visible under `cello.version`, which cannot collide with an application key. Built-in keys win over colliding entries, and keys and values are percent-decoded before export. Precedence for the service name is env (`OTEL_SERVICE_NAME`), then resource attributes, then the `cello` default. Behavior change: without an operator-set version there is no `service.version` at all — reporting Cello's version there told every deployment-tracking backend that all applications run Cello's build ([upstream #314](https://github.com/denoland/celld/issues/314)).

## Correlate a request end to end

- `0133` — Your handler can read the traceparent of the span it runs in through `getCurrentTraceparent()` from `cloudflare:workers`, so it can return the value for end-to-end tracing ([upstream #313](https://github.com/denoland/celld/issues/313)).
- `0134` — Incoming spans carry the HTTP method and the response status, so worker spans and cell-fetch spans tell apart in export ([upstream #285](https://github.com/denoland/celld/issues/285)).

## Send logs and traces to different sinks

- `0136` — `CELLO_OTEL_LOGS` splits the log sink from the traces sink: unset means logs follow the traces sink (the historical behavior); `bucket` sends logs to bucket Parquet whatever the traces sink is; `0` drops logs while traces keep exporting; any other value refuses startup. Point your collector at traces while your logs land in the bucket ([upstream #153](https://github.com/denoland/celld/issues/153)).
