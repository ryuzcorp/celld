---
title: Limitations
description: "The operational limits of a Cello fleet: one application, external TLS, credentials, and reconnects."
---

# Limitations

Cello v0.6.3 is a beta release with these operational limits. See
[Cloudflare compatibility](cloudflare-compat.md) for the supported
services, APIs, and Wrangler configuration.

## Run one application per fleet

- A fleet runs one application. Cello has no account service,
  multi-tenant scheduler, or managed ingress.
- A fleet stores its durable state in an S3-compatible bucket, a Google
  Cloud Storage bucket, or an Azure Blob Storage container. Only
  `cello dev` uses the local SQLite object store.
- Ownership balancing counts cells by node weight, not by CPU or memory
  use. It moves only hibernated cells, so a fleet without idle eviction
  balances only the cells that hibernate on their own.

## Terminate TLS outside Cello

- Cello does not terminate TLS. Terminate public TLS at an ingress proxy,
  and put the internal listener on a private network or an encrypted
  overlay.
- Peer traffic is plaintext HTTP. The fleet HMAC authenticates tunnel
  establishment and control requests but does not encrypt data, so your
  network must provide confidentiality.
- The fleet bucket controls the fleet. Give its credentials access to one
  fleet only. See [Security](../guides/security.md).

## Match credentials to your provider

- The credential methods differ by provider. See
  [Configure storage](../guides/storage.md).
- Azure identity works only in the public Azure cloud. A managed identity
  from Azure App Service or Azure Container Apps does not work. Use a
  workload identity or a storage account key there.

## Reconnect WebSockets after a move

- An outbound Durable Object WebSocket keeps its cell resident. It closes
  when the cell moves to another node, so store the connection intent and
  reconnect.
- A node limits the resident cells and outbound WebSockets.

## Stream a response for as long as the client listens

- A streaming response body is response work, not background work.
  `CELLO_BACKGROUND_BUDGET_S` ends `ctx.waitUntil()` work after the handler
  answers, but never a body the client is still reading. The body runs while
  the client stays connected and the body keeps producing. While a body
  streams, the budget ends the `waitUntil` claim; timers that work already
  started stop one budget window after the body ends.
- An open response still ends: when the body completes or errors, when the
  client disconnects (which cancels the producer), after `CELLO_HANDLER_BUDGET_S`
  if the handler has not answered, when the node shuts down, and after 60
  seconds without progress: the body produces nothing, or the client stops
  reading.
- A streamed body keeps its event, isolate, and cell resident until it ends,
  so a retiring generation waits for open streams before it frees its heaps.
- A `TransformStream` applies backpressure: while nobody reads the readable
  side, `await writer.write(...)` stays pending and `writer.ready` does not
  settle, so a response body built from one stops producing when the HTTP
  client stops reading instead of buffering in the isolate's heap. A
  cancelled readable, an aborted or errored writable, and
  `controller.error()` / `controller.terminate()` release a parked write
  instead of leaving it pending forever. A
  producer that writes without awaiting either, or that writes after
  `writer.desiredSize` says 0, still queues its chunks in memory — Cello
  cannot bound a stream you keep feeding.
- A `tee()` bounds the skew between its branches: the difference between
  what the two branches hold is measured just after a chunk is queued, and
  past 32 MiB — the budget the host tee of a platform body uses — the
  branch that is behind fails its next read with `tee branch fell more than
  32 MiB behind its sibling; read both branches concurrently`, while the
  branch being read keeps flowing to the end. The chunk size alone fails no
  branch, but a branch that has not read an oversize chunk yet holds all of
  it and fails for lag, so with chunks larger than the budget both branches
  must be read concurrently. A chunk is charged its size — a string 2 bytes
  per UTF-16 code unit, a buffer its length, a view its visible length and
  not the buffer behind it — and every chunk at least 64 bytes. That makes
  the budget a guard against accidental buffering rather than an isolate
  memory limit: a payload hidden in an object chunk, or a small view over a
  large buffer, is not counted. The budget is per `tee()`, so a chain of N
  clones can hold N × 32 MiB. Read both branches, or cancel the one you do
  not need.
- An isolate holds at most 128 live `CompressionStream`s and
  `DecompressionStream`s together. Constructing one more throws a
  `RangeError`. A stream stops counting once it finishes, is cancelled or
  aborted, errors, or is garbage-collected after it becomes unreachable.

## Run on Linux or Apple Silicon

The installer supplies binaries for Linux x86-64, Linux ARM64, and Apple
Silicon. Windows is not supported.
