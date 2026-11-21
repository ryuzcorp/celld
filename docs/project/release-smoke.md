---
title: Release smoke gate
description: Run the scripted stream and large-payload checks against a published Cello image before a consumer pins it.
---
<!-- Modified by Ryuz Corp (Cello): release smoke gate documentation. -->

A release image passes the release smoke gate before any consumer pins it. The gate drives the shapes a consumer depends on — long-lived streams, WebSocket messages at the size cap, an 8 MiB `tee()`, and backpressure — against the image itself, so a regression that only shows up in a consumer dogfood fails here first.

Run it after [`publish.yml`](https://github.com/ryuzcorp/cello/blob/main/.github/workflows/publish.yml) publishes a tag, and before a consumer changes its pin:

```sh
scripts/release-smoke.sh ghcr.io/ryuzcorp/cello:0.2.3@sha256:<digest>
```

Pass any image reference: a published digest, a tag, or a locally built tag. The gate starts the image under Docker as a `cello-releasesmoke-*` container, serves the fixture Worker in `scripts/release-smoke/` through `cello dev`, drives the checks from the host, and removes the container on every exit path. It prints `cello --version` from the image before the first check.

## What it checks

| # | Check | Pass condition |
| --- | --- | --- |
| 1 | SSE through a `TransformStream` response | A frame every 5 s, first-to-last span at least 300 s |
| 2 | A response body that never produces | The 60 s idle deadline ends it, 55–75 s after the request |
| 3 | 20 MiB WebSocket message | The echo matches the sent bytes by sha256 |
| 4 | 32 MiB WebSocket message | Exactly 32 MiB passes; 32 MiB + 1 closes with 1009 |
| 5 | 8 MiB body `tee()`d | Branch A drained fully, then B; both match the expected sha256 |
| 6 | A client that stops reading | The producer's completed-write count plateaus (at most 192 × 128 KiB) |

Each check prints one `PASS`/`FAIL` line with its elapsed time and detail, then a summary. The gate exits non-zero on any failure and prints the container log tail.

## Requirements and knobs

The host needs Docker (reached through the `docker` group with `/usr/bin/sg`), `curl`, and Node.js 24 or newer. The client in `scripts/release-smoke/client.mjs` uses only Node built-ins; it needs no npm packages. The image itself needs nothing extra: the fixture sets `no_bundle`, so the image's missing esbuild is never invoked.

| Variable | Effect |
| --- | --- |
| `SMOKE_SSE_SECONDS` | SSE span the gate requires; default `300` |
| `SMOKE_QUICK=1` | Shorten the SSE span to 90 s for local iteration |

The gate runs the default span, not `SMOKE_QUICK`, when it clears an image for a consumer pin. Checks 1 and 2 run together, so a full run takes about six minutes.

## When it fails

A failure names the check and the observed value — the span an SSE body survived, the close code an oversize message produced, the write count a stalled producer reached. Reproduce it against the same image tag before the pin moves, and treat the failure as a release blocker: the image is what consumers run, so a red gate means the pin waits.
