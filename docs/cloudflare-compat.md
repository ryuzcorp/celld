<!-- Modified by ryuzcorp (ryuzcorp/celld): node:crypto scrypt; cloned streaming bodies; cross-isolate RPC stubs and streams; per-node Workers Cache API; receiver-held RPC streams. -->
# Cloudflare compatibility

This page lists only the differences from the linked Cloudflare APIs. An entry
without a note matches Cloudflare.

- **Yes**: implemented, except for the listed differences.
- **Partial**: a substantial part is unavailable.
- **Experimental**: can change without notice.
- **No**: not implemented.

celld rejects an unsupported configuration or API at deployment or first use.
An unsupported feature that does not cause an error is a defect.

## Services

| service | status |
| --- | --- |
| [Workers](services/workers.md) | **Yes** |
| [Durable Objects](services/durable-objects.md) | **Yes** |
| [Durable Object Facets](services/durable-object-facets.md) | **Yes** |
| [Containers](services/containers.md) | **Experimental** |
| [Static assets](services/static-assets.md) | **Yes** |
| [Cron Triggers](services/cron-triggers.md) | **Yes** |
| [Dynamic Workers](services/dynamic-workers.md) | **Yes** |
| [KV](services/kv.md) | **Yes** |
| [Queues](services/queues.md) | **Yes** |
| [D1](services/d1.md) | **Yes** |
| [Workflows](services/workflows.md) | **Yes** |
| [R2](services/r2.md) | **Yes** |
| Workers AI | **No** |
| Vectorize | **No** |
| Hyperdrive | **No** |
| Browser Rendering | **No** |
| Email Workers | **No** |
| [Python Workers](services/workers.md#python-workers) | **Partial** — `fetch` handlers on the Pyodide 0.28 runtime line |

## Runtime APIs

| API | status |
| --- | --- |
| [Fetch, Request, Response, and Headers](#fetch-request-response-and-headers) | **Yes** |
| [Bindings](#bindings) | **Yes** |
| [Context](#context) | **Yes** |
| [Handlers](#handlers) | **Yes** |
| [RPC](#rpc) | **Yes** |
| [Streams](#streams) | **Yes** |
| Encoding | **Yes** |
| [WebSockets](#websockets) | **Yes** |
| [Web Crypto](#web-crypto) | **Yes** |
| [Web standards](#web-standards) | **Yes** |
| WebAssembly | **Yes** |
| [Performance and timers](#performance-and-timers) | **Yes** |
| Console | **Yes** |
| [Node.js compatibility](#nodejs-compatibility) | **Partial** |
| [Cache](#cache) | **Yes** |
| HTMLRewriter | **Yes** |
| [TCP sockets](#tcp-sockets) | **Yes** |
| EventSource | **Yes** |
| MessageChannel | **Yes** |
| BroadcastChannel | **No** |

### [Fetch, Request, Response, and Headers](https://developers.cloudflare.com/workers/runtime-apis/fetch/)

- The `cache` request option is unavailable.
- An inbound `Request` has a `cf` object without Cloudflare edge fields. celld
  cannot prove the geolocation, colo, or TLS metadata.
- celld removes `Content-Length` from a Worker response, except for a `HEAD`
  response.
- A cloned body that the platform streams (a `fetch()`, RPC, Durable Object
  or R2 body) applies backpressure at the slower copy: reading only one copy
  stalls it after 16 chunks until the other copy is read, cancelled, or
  expires. Workerd buffers for the slower copy instead.
- A remote Durable Object call cannot retry after request-body transmission
  starts because celld keeps no replay copy.
- A remote Durable Object call waits for a rejected owner generation to change
  for at most `CELLD_OPERATION_DEADLINE_MS`.

### [Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/)

Only the binding types in the [services table](#services) are available.

### [Context](https://developers.cloudflare.com/workers/runtime-apis/context/)

- `passThroughOnException()` has no effect because celld has no CDN fallback.
- `ctx.facets` is available only inside a Durable Object.

### [Handlers](https://developers.cloudflare.com/workers/runtime-apis/handlers/)

The `tail` and `email` handlers are unavailable.

### [RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)

- Durable Object RPC stubs cross isolate boundaries on the same node: a
  method called on a stub returned by another isolate runs against the
  owning cell. A stub whose owner runs on another node rejects with a
  clear error (no cross-node forwarding yet); a stub whose owner was
  evicted or reloaded rejects like a released same-isolate stub. Old-shape
  markers (no host id) keep failing on use exactly as before.
- A stub received by a caller is owned by the caller's
  request: when the fetch/RPC/alarm/queue invocation ends, every stub it
  still holds is disposed and every release routes to the owner. There is
  no idle expiry: a quiet-but-live handle stays usable however long
  nothing flows, and a validated operation pins its entry so a release
  racing it cannot fail it. Owner eviction revokes synchronously, and
  isolate retirement releases whatever the freed isolate still held.
- A stream received over RPC stays usable while the receiver holds it:
  an active reader (or writer) keeps the stream past the receiving call
  and its invocation, the way a ReadableStream sent over RPC stays open
  until its writer closes it. Reading to the end, closing, cancelling,
  or an explicit dispose releases the hold at once; a dropped stream
  frees when collected, or when its isolate retires.
- A caller-owned `ReadableStream` passed in call arguments crosses to the
  cell through an upload channel: the caller's isolate pumps it host-side
  (bounded, backpressured) and the cell pulls. Cancellation propagates
  both ways, and the channel dies with the sender's invocation. A
  caller-owned `WritableStream` has no reverse path: it keeps a
  same-isolate marker and fails on use across isolates.
- Minting is capped per scope (4096 live stubs) and per process (65536
  stubs, 8192 uploads), failing the call with a clear error past the cap.
  `CELLD_STUB_SCOPE_CAP`, `CELLD_STUB_PROCESS_CAP`, and `CELLD_UPLOAD_CAP`
  override the caps. Stub ids are 128-bit bearer tokens bound to the
  isolate that revived them; a stolen id alone is useless, and ids never
  log or serialize observably. Upload ids are process-wide bearer tokens
  without holder binding (any isolate presenting one can pull, push, or
  cancel it), contained by the same unguessability, uniform errors, and
  non-observability: markers never surface to script, and forwarding
  moves the wrapper rather than the id. One celld process is one fleet,
  so the bearer boundary is the process.
- An `AbortSignal` in a Durable Object RPC call crosses on the same node
  (live: the receiver observes an abort the sender fires later) but does
  not cross a node boundary.
- A remote RPC retries only when the failed peer attempt did not start the
  method. Use a stable operation ID for an application retry.
- Deviations from workerd stub lifetime: a stub used only from `waitUntil`
  work after the handler settles is already disposed; a stub a Durable
  Object stores on its instance does not survive into a later invocation;
  a stub does not survive a socket upgrade. Each fails closed with a
  disposed error instead of leaking.
- `globalThis.__celldStubStatsForTestOnly()` reports this isolate's stub
  counts for the conformance suite. It exists only on nodes booted with
  `CELLD_TEST_STUB_STATS=1` (a process environment variable, which tenant
  code cannot set); production isolates never define it. It is test
  tooling, not a runtime API.

### [Streams](https://developers.cloudflare.com/workers/runtime-apis/streams/)

- celld expires an unclaimed and inactive HTTP stream after 60 seconds. A
  successful stream operation starts a new 60-second period.
- An expired or unknown stream reports an error instead of EOF.
- Byte streams cross isolate boundaries on the same node as part of Durable
  Object RPC, in both directions: a `ReadableStream` returned by a cell
  reads chunk by chunk, and a `WritableStream` it returns accepts writes,
  closes, and aborts. A `ReadableStream` passed in call arguments crosses
  the other way, pumped caller-side through a bounded host channel the
  cell pulls (see RPC above). Only byte chunks cross that channel: a
  non-bytes chunk fails the stream with a disconnect rather than
  mangling it. Nothing here expires on a timer.

### [WebSockets](https://developers.cloudflare.com/workers/runtime-apis/websockets/)

- An outbound Worker socket closes when its event and `waitUntil` work end. A
  socket returned in the response stays open.
- Each isolate-polled input queue has a 1 MiB budget for non-terminal frames. A
  message larger than 1 MiB uses the complete budget.
- If the isolate stops polling, celld discards unread frames during cleanup. A
  later pull reports an abnormal close.
- A WebSocket transport cannot move to a new cell owner. A client must reconnect
  with the same application operation ID.
- A tunneled connection forwards the owner's Close without an additional Close.
  If the owner connection fails between frames before a Close, the ingress sends
  code 1012. If a frame is incomplete, the ingress closes the transport instead.
- `acceptWebSocket()` throws above 90 percent of the V8 heap limit.
- A response with a WebSocket to a request without `Upgrade: websocket` fails,
  as in workerd. A Durable Object's `stub.fetch()` call then rejects, and an
  HTTP client receives status 500. The server end of the socket receives a close
  event with code 1006.
- A Durable Object's `stub.fetch()` call rejects when the handler fails, on the
  owner node and on any other node.

### [Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/)

- HMAC accepts MD5, SHA-1, SHA-224, SHA-256, SHA-384, and SHA-512.
- ECDSA supports only the P-256 curve with SHA-256.
- AES-GCM accepts authentication tags from 96 through 128 bits in 8-bit steps.
- RSA-OAEP accepts SHA-1, SHA-256, SHA-384, and SHA-512. A nonempty label must
  contain valid UTF-8.
- A secret key cannot use `jwk` with `exportKey()` or `wrapKey()`.
- Ed25519 signs and verifies, and `NODE-ED25519` names the same algorithm. A
  signature from one spelling verifies under the other.
- X25519 derives bits and keys. A peer key of a low order gives a shared secret
  of all zero bytes, so `deriveBits()` rejects that key instead.
- An Ed25519 or X25519 public key uses its 32-byte point for the `raw` format,
  so `importKey()` and `exportKey()` both carry the point alone.

### [Web standards](https://developers.cloudflare.com/workers/runtime-apis/web-standards/)

### [Performance and timers](https://developers.cloudflare.com/workers/runtime-apis/performance/)

`performance.timeOrigin` is `0`, and `performance.now()` matches `Date.now()`.
Both clocks advance at an I/O boundary and stay fixed during JavaScript
execution.

### [Node.js compatibility](https://developers.cloudflare.com/workers/runtime-apis/nodejs/)

- celld implements `node:assert`, `node:async_hooks`, `node:buffer`,
  `node:diagnostics_channel`, `node:events`, `node:fs`, `node:os`, `node:path`,
  `node:stream`, `node:timers/promises`, and `node:util`.
- `node:diagnostics_channel` does not export messages to a tail Worker.
- `node:crypto` does not implement Diffie-Hellman, streaming signatures,
  ciphers, RSA-PSS, or DSA signatures and key generation.
- `node:crypto` implements `scrypt`/`scryptSync` with Node's parameters
  and defaults (`N`/`cost` 16384, `r`/`blockSize` 8, `p`/`parallelization`
  1, `maxmem` 32 MiB).
- `KeyObject.toCryptoKey()` applies the requested algorithm, extractability,
  and usages to an asymmetric key.
- `node:zlib` implements only the synchronous gzip and deflate functions.
- `node:fs` provides `access`, `mkdir`, `realpath`, `stat`, `lstat`, and
  `readFile`. It exposes an empty, request-local `/tmp` and a read-only
  `/bundle` that contains the Worker modules.
- The global `process` matches workerd for each field that it defines, such
  as `process.execPath`, `process.argv`, and `process.title`. Other fields,
  such as `process.kill` and `process.features`, are undefined.
- The celld bundler supports a synchronous CommonJS `require()` of a Node.js
  built-in module. A raw ESM Worker has no global `require()`.
- A Node.js built-in module object is writable, so a dependency such as
  `graceful-fs` can patch it at load. The patch stays in its isolate.
- An import of another Node.js module succeeds, but its first call throws an
  error.

### [Cache](https://developers.cloudflare.com/workers/runtime-apis/cache/)

See [Cache](services/cache.md) for the full behavior, freshness rules, and
limits.

- The cache is per-node and in-memory: entries survive across isolates of
  one script on one node, never cross scripts, and never cross nodes.
- `fetch()` neither reads nor populates the cache, and `cf.cacheKey` is
  unsupported; the request URL is the whole key.
- No API purges by `Cache-Tag`; purge by URL with `delete`.

### [TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)

- A socket cannot outlive its event. A Durable Object must reconnect during a
  later event.
- celld verifies a TLS server against its bundled Mozilla root store.
- celld does not block the destination ports that Cloudflare blocks. The fleet
  network controls the egress policy.

### BroadcastChannel

The class exists so that a bundle can load, but its constructor throws.

## Compatibility flags

celld honors these compatibility flags:

- `delete_all_deletes_alarm`
- `js_rpc`
- `fetcher_no_get_put_delete`
- `sqlite_vec`
- `websocket_standard_binary_type`
- The static-assets navigation flags

celld accepts every other flag without effect.
`Cloudflare.compatibilityFlags` reports only the honored flags.

## Wrangler configuration

`celld deploy` accepts `wrangler.jsonc` or `wrangler.json`, not
`wrangler.toml`.

The `name` value must contain 1 to 63 lowercase ASCII letters, digits, or
internal hyphens. It cannot start or end with a hyphen.

The accepted top-level keys:

- `$schema`, `name`, `main`, and `no_bundle`
- `compatibility_date` and `compatibility_flags`
- `durable_objects` and `migrations`
- `assets`, `services`, `triggers`, and `vars`
- `d1_databases`, `kv_namespaces`, `queues`, `workflows`, and
  `r2_buckets`
- `worker_loaders` and `containers`
- `define` and `rules`

Any other top-level key, including `routes`, stops the deployment.

`define` and `rules` go to the esbuild run. Each `define` value is a
JavaScript expression, as in Wrangler. A rule `type` must be `Text`, `Data`,
or `CompiledWasm`, and each glob must have the form `**/*.ext` or `*.ext`,
because esbuild selects a loader by extension. Two rules that give one
extension different types stop the deployment. celld already applies
`CompiledWasm` to `**/*.wasm`, so only that type is valid for `.wasm`.
`no_bundle` skips esbuild, so `no_bundle` with `define` or `rules` stops the
deployment.

An asset-only project can omit `main`. `celld deploy` refuses an unsafe asset
path, and `.assetsignore` requires Wrangler.

See [Limitations](limitations.md) for the operational limits.
