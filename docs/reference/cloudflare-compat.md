---
title: Cloudflare compatibility
description: Which Cloudflare services and runtime APIs Cello supports, and how each difference behaves.
---
<!-- Modified by Ryuz Corp (Cello): node:crypto scrypt; cloned streaming bodies; cross-isolate RPC stubs and streams; per-node Workers Cache API; receiver-held RPC streams; AbortSignal.timeout is unreferenced (upstream #306); loader 101 webSocket target (upstream #305); loader sibling cloudflare:workers imports (upstream #304); AES-KW and symmetric JWK (upstream #295); EC raw point format (upstream #271); writable request /tmp, node:url file helpers, and the CELLO_TMP_MAX_BYTES /tmp cap (upstream #280); workerd close codes (upstream #279); fetch gzip/deflate decoding (upstream #238); Symbol.dispose on RPC results (upstream #260); post-response DO dial (upstream #254); synchronous caller cancel (upstream #281). -->

This page lists only the differences from the linked Cloudflare APIs. An
entry without a note matches Cloudflare.

- **Yes**: implemented, except for the listed differences.
- **Partial**: a substantial part is unavailable.
- **Experimental**: can change without notice.
- **No**: not implemented.

Cello rejects an unsupported configuration or API at deployment or first
use. An unsupported feature that stays silent is a defect.

## Services

| Service | Status | Differences |
| --- | --- | --- |
| [Workers](../services/workers.md) | **Yes** | No custom domains or TLS termination; `passThroughOnException()` has no effect; no Workers AI binding |
| [Durable Objects](../services/durable-objects.md) | **Yes** | See the service page |
| [Durable Object Facets](../services/durable-object-facets.md) | **Yes** | See the service page |
| [Containers](../services/containers.md) | **Experimental** | See the service page |
| [Static assets](../services/static-assets.md) | **Yes** | See the service page |
| [Cron Triggers](../services/cron-triggers.md) | **Yes** | See the service page |
| [Dynamic Workers](../services/dynamic-workers.md) | **Yes** | See the service page |
| [KV](../services/kv.md) | **Yes** | See the service page |
| [Queues](../services/queues.md) | **Yes** | See the service page |
| [D1](../services/d1.md) | **Yes** | See the service page |
| [Workflows](../services/workflows.md) | **Yes** | See the service page |
| [R2](../services/r2.md) | **Yes** | See the service page |
| [Cache](../services/cache.md) | **Yes** | Per-node and in-memory; no `fetch()` integration; purge by URL, not by tag |
| Workers AI | **No** | Not implemented |
| Vectorize | **No** | Not implemented |
| Hyperdrive | **No** | Not implemented |
| Browser Rendering | **No** | Not implemented |
| Email Workers | **No** | Not implemented |
| [Python Workers](../services/workers.md#python-workers) | **Partial** | `fetch` handlers on the Pyodide 0.28 runtime line |

## Runtime APIs

| API | Status |
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
- A gzip or deflate response body decodes before delivery, unless your
  request names `accept-encoding` (then the body arrives raw, as the Fetch
  standard requires). `content-encoding` stays on the response, as in
  workerd; the wire size no longer describes the body, so `content-length`
  is removed.
- An inbound `Request` carries a `cf` object without Cloudflare edge
  fields. Cello cannot prove the geolocation, colo, or TLS metadata.
- Cello removes `Content-Length` from a Worker response, except for a
  `HEAD` response.
- A cloned body that the platform streams (a `fetch()`, RPC, Durable
  Object, or R2 body) buffers for the slower copy up to 32 MiB of skew,
  shared by all of one request's tees. Past that, the lagging copy's next
  read rejects with `tee branch fell more than 32 MiB behind its sibling;
  read both branches concurrently`, and the other copies keep flowing to
  completion. Workerd buffers for the slower copy within the isolate's
  128 MB limit instead of failing it.
- A remote Durable Object call cannot retry after request-body
  transmission starts, because Cello keeps no replay copy.
- A remote Durable Object call waits for a rejected owner generation to
  change for at most `CELLO_OPERATION_DEADLINE_MS`.

### [Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/)

Only the binding types in the [services table](#services) are available.

`env` additionally carries `CELLO_NODE`: the name of the node running your
code, injected by the host after every deployment binding. A bundle that
must refuse a credential minted for another region reads this, not
placement. No deployment may bind the name — `vars`, `.dev.vars`, and
every other binding kind are refused at deploy time — and a loaded
Worker's props cannot smuggle it past the host value of an isolate that
has one.

In a Worker Loader bundle (`env.LOADER`), a `cloudflare:workers` name
imported only by a non-entry chunk still resolves: external stubs cover
the union of the main module's and every sibling's imports.

### [Context](https://developers.cloudflare.com/workers/runtime-apis/context/)

- `passThroughOnException()` has no effect, because Cello has no CDN
  fallback.
- `ctx.facets` is available only inside a Durable Object.

### [Handlers](https://developers.cloudflare.com/workers/runtime-apis/handlers/)

The `tail` and `email` handlers are unavailable.

### [RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)

- Durable Object RPC stubs cross isolate boundaries on the same node: a
  method you call on a stub returned by another isolate runs against the
  owning cell. A stub whose owner runs on another node rejects with a
  clear error (no cross-node forwarding yet); a stub whose owner was
  evicted or reloaded rejects like a released same-isolate stub. Old-shape
  markers (no host id) keep failing on use exactly as before.
- The caller's request owns a stub it receives: when the fetch, RPC,
  alarm, or queue invocation ends, Cello disposes every stub it still
  holds and routes every release to the owner. There is no idle expiry: a
  quiet-but-live handle stays usable however long nothing flows, and a
  validated operation pins its entry so a racing release cannot fail it.
  Owner eviction revokes synchronously, and isolate retirement releases
  whatever the freed isolate still held.
- A stream you receive over RPC stays usable while you hold it: an active
  reader (or writer) keeps the stream past the receiving call and its
  invocation, the way a ReadableStream sent over RPC stays open until its
  writer closes it. Reading to the end, closing, cancelling, or an
  explicit dispose releases the hold at once; a dropped stream frees when
  collected, or when its isolate retires.
- A caller-owned `ReadableStream` in call arguments crosses to the cell
  through an upload channel: your isolate pumps it host-side (bounded,
  backpressured) and the cell pulls. Cancellation propagates both ways,
  and the channel dies with the sender's invocation. A caller-owned
  `WritableStream` has no reverse path: it keeps a same-isolate marker and
  fails on use across isolates.
- Minting is capped per scope (4096 live stubs) and per process (65536
  stubs, 8192 uploads); calls past the cap fail with a clear error.
  `CELLO_STUB_SCOPE_CAP`, `CELLO_STUB_PROCESS_CAP`, and
  `CELLO_UPLOAD_CAP` override the caps. Stub ids are 128-bit bearer
  tokens bound to the isolate that revived them; a stolen id alone is
  useless, and ids never log or serialize observably. Upload ids are
  process-wide bearer tokens without holder binding (any isolate
  presenting one can pull, push, or cancel it), contained by the same
  unguessability, uniform errors, and non-observability: markers never
  surface to script, and forwarding moves the wrapper rather than the id.
  One Cello process is one fleet, so the bearer boundary is the process.
- An `AbortSignal` in a Durable Object RPC call crosses on the same node
  (live: the receiver observes an abort the sender fires later) but does
  not cross a node boundary. Cancelling publishes synchronously, so the
  abort reaches the callee even as the caller's entry retires.
- A remote RPC retries only when the failed peer attempt did not start
  the method. Use a stable operation ID for your own retry.
- A plain object or array returned over RPC carries a no-op
  `Symbol.dispose`, so a `using` declaration typechecks; only results that
  revive stubs or disposers release anything.
- Workerd stub lifetimes differ in three cases: a stub you use only from
  `waitUntil` work after the handler settles is already disposed; a stub
  a Durable Object stores on its instance does not survive into a later
  invocation; a stub does not survive a socket upgrade. Each fails closed
  with a disposed error instead of leaking.
- `globalThis.__celloStubStatsForTestOnly()` reports your isolate's stub
  counts for the conformance suite. It exists only on nodes booted with
  `CELLO_TEST_STUB_STATS=1` (a process environment variable, which tenant
  code cannot set); production isolates never define it. It is test
  tooling, not a runtime API.

### [Streams](https://developers.cloudflare.com/workers/runtime-apis/streams/)

- Cello expires an unclaimed and inactive HTTP stream after 60 seconds. A
  successful stream operation starts a new 60-second period.
- An expired or unknown stream reports an error instead of EOF.
- Byte streams cross isolate boundaries on the same node as part of
  Durable Object RPC, in both directions: a `ReadableStream` your cell
  returns reads chunk by chunk, and a `WritableStream` it returns accepts
  writes, closes, and aborts. A `ReadableStream` in call arguments
  crosses the other way, pumped caller-side through a bounded host
  channel the cell pulls (see RPC above). Only byte chunks cross that
  channel: a non-bytes chunk fails the stream with a disconnect rather
  than mangling it. Nothing here expires on a timer.

### [WebSockets](https://developers.cloudflare.com/workers/runtime-apis/websockets/)

- An outbound Worker socket closes when its event and `waitUntil` work
  end. A socket returned in the response stays open.
- Each isolate-polled input queue has a 1 MiB budget for non-terminal
  frames. A message larger than 1 MiB uses the complete budget.
- If your isolate stops polling, Cello discards unread frames during
  cleanup. A later pull reports an abnormal close.
- Each pipe direction between two ends is bounded twice: at most 16 MiB
  may wait in its output channel (the admission cap), and at most 16 MiB
  may sit staged upstream of its destination (the staging cap) — another
  isolate's input queue, a cell dispatch, or the socket itself. Admission
  bytes release when the pipe's forwarder receives a frame, and staged
  bytes when its send completes, so a direction with a full staging queue
  can still admit up to another 16 MiB into its output channel: the two
  limits are separate, not one 16 MiB aggregate. Either cap polices only
  bytes already held ahead of each frame: one message up to the 32 MiB
  message cap below passes whatever small backlog precedes it, so a
  heartbeat staged ahead never dooms the large message behind it. A
  backlog already at either cap still ends the pipe loudly, as does a
  frame larger than one message. Each ledger therefore holds at most just
  under its cap plus one message, so the transient worst case per
  direction is just under twice that (about 96 MiB). A flood past either
  cap ends the pipe loudly instead of growing host memory without bound:
  both ends observe close code 1011 with `wasClean: false`, an abnormal
  host-originated close no peer sent. Only staged bytes count against the
  cap: handler runtime stays unbounded, so a slow `webSocketMessage`
  handler never trips it, and a slow but live peer still drains fully —
  ordinary data writes never time out. Only the closing writes run under
  a 5-second progress bound — the close flush, the automatic ping/pong and
  close replies, and the loud-1011 attempts — and each frame delivered
  restarts the wait.
- A single WebSocket message — one frame or fragments assembled — is
  bounded at 32 MiB, Cloudflare's own message size limit: a peer that
  fragments past it ends the socket loudly with close code 1009 (Message
  Too Big) instead of growing host memory without bound, and your cell
  observes the 1009 as an abnormal application close. Each frame is held
  to what its message has left, so a frame that would pass the cap is
  refused from its header before its payload is read. A single message up
  to this cap also passes each 16 MiB pipe stage above whatever small
  backlog precedes it. Bytes held while a message is read do not count
  against the 16 MiB staging cap above: up to 32 MiB of assembly plus the
  WebSocket parser's read buffer, which can keep about twice the largest
  frame (about 64 MiB). The worst case per socket is therefore under about
  100 MiB plus one staged budget and the message ahead of it (just under
  16 MiB plus one message).
- Socket delivery is best-effort: if a 1011 attempt times out, the local
  pipe still reports 1011 but the wire peer may see an abnormal
  disconnect with no Close frame. Likewise, a write that stops mid-frame
  poisons the socket: no further frame is ever written after it, and the
  transport drops instead — the peer sees a truncation, never a terminal
  frame parsed as a continuation of the unfinished one.
- A close queued behind data still delivers every frame first, in order,
  while the peer can make progress; a peer that stopped reading ends the
  pipe instead of blocking closure.
- A WebSocket transport cannot move to a new cell owner. Your client must
  reconnect with the same application operation ID.
- A tunneled connection forwards the owner's Close without an additional
  Close. If the owner connection fails between frames before a Close, the
  ingress sends code 1012. If a frame is incomplete, the ingress closes
  the transport instead.
- `acceptWebSocket()` throws above 90 percent of the V8 heap limit.
- A response with a WebSocket to a request without `Upgrade: websocket`
  fails, as in workerd. Your Durable Object's `stub.fetch()` call then
  rejects, and an HTTP client receives status 500. The server end of the
  socket receives a close event with code 1006.
- Your Durable Object's `stub.fetch()` call rejects when the handler
  fails, on the owner node and on any other node.
- A Worker Loader worker (`env.LOADER`) answers a WebSocket upgrade the
  same way: a `101` response carrying `webSocket` routes to its target
  whether the loaded worker made the socket itself or forwards the
  upgrade to a Durable Object.
- `close()` sends 1000–4999 except the reserved no-status codes 1004,
  1005, 1006, and 1015, which stay refused. Codes 1001–1014 that the old
  WHATWG-only rule rejected now close, so a 1008 policy-violation refusal
  reaches the peer instead of throwing.
- A Durable Object can open an outbound WebSocket after it answers: the
  dial routes by the object's cell, not by the dead request.

### [Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/)

- HMAC accepts MD5, SHA-1, SHA-224, SHA-256, SHA-384, and SHA-512.
- ECDSA supports only the P-256 curve with SHA-256.
- AES-GCM accepts authentication tags from 96 through 128 bits in 8-bit
  steps.
- RSA-OAEP accepts SHA-1, SHA-256, SHA-384, and SHA-512. A nonempty label
  must contain valid UTF-8.
- AES-KW wraps and unwraps (RFC 3394; an IV mismatch fails the unwrap). A
  symmetric key imports and exports as `raw` bytes and as `jwk` with
  `kty: "oct"`.
- Ed25519 signs and verifies, and `NODE-ED25519` names the same
  algorithm. A signature from one spelling verifies under the other.
- X25519 derives bits and keys. A peer key of a low order gives a shared
  secret of all zero bytes, so `deriveBits()` rejects that key instead.
- An Ed25519 or X25519 public key uses its 32-byte point for the `raw`
  format, so `importKey()` and `exportKey()` both carry the point alone.
- An EC public key imports and exports the uncompressed point as `raw`.

### [Web standards](https://developers.cloudflare.com/workers/runtime-apis/web-standards/)

`AbortSignal.timeout()` never keeps its event (or its admission slot)
alive: the timer is unreferenced, so it fires or dies with the request,
whichever comes first, and carries no timer id or cancellation.

### [Performance and timers](https://developers.cloudflare.com/workers/runtime-apis/performance/)

`performance.timeOrigin` is `0`, and `performance.now()` matches
`Date.now()`. Both clocks advance at an I/O boundary and stay fixed
during JavaScript execution.

### [Node.js compatibility](https://developers.cloudflare.com/workers/runtime-apis/nodejs/)

- Cello implements `node:assert`, `node:async_hooks`, `node:buffer`,
  `node:diagnostics_channel`, `node:events`, `node:fs`, `node:os`,
  `node:path`, `node:stream`, `node:timers/promises`, and `node:util`.
- `node:diagnostics_channel` does not export messages to a tail Worker.
- `node:crypto` does not implement Diffie-Hellman, streaming signatures,
  ciphers, RSA-PSS, or DSA signatures and key generation.
- `node:crypto` implements `scrypt`/`scryptSync` with Node's parameters
  and defaults (`N`/`cost` 16384, `r`/`blockSize` 8,
  `p`/`parallelization` 1, `maxmem` 32 MiB).
- `KeyObject.toCryptoKey()` applies the requested algorithm,
  extractability, and usages to an asymmetric key.
- `node:zlib` implements only the synchronous gzip and deflate functions.
- `node:fs` provides `access`, `mkdir`, `realpath`, `stat`, `lstat`,
  `readFile`, `writeFile`, `writeFileSync`, `appendFile`,
  `appendFileSync` (plus `fs.promises`), and read/write streams. `/tmp`
  is writable and request-local: `writeFile` truncates unless
  `{ flag: "a" }` appends, `createWriteStream` writes chunks as they
  arrive, and anything that would push a request past
  `CELLO_TMP_MAX_BYTES` (default 64 MiB) fails with `ENOSPC`, while an
  overwrite frees the replaced bytes. `/bundle` is read-only and holds
  the Worker modules.
- `node:url` provides `pathToFileURL`/`fileURLToPath` plus the WHATWG URL
  API, with POSIX path rules only.
- The global `process` matches workerd for each field that it defines,
  such as `process.execPath`, `process.argv`, and `process.title`. Other
  fields, such as `process.kill` and `process.features`, are undefined.
- The Cello bundler supports a synchronous CommonJS `require()` of a
  Node.js built-in module. A raw ESM Worker has no global `require()`.
- A Node.js built-in module object is writable, so a dependency such as
  `graceful-fs` can patch it at load. The patch stays in its isolate.
- An import of another Node.js module succeeds, but its first call throws
  an error.

### [Cache](https://developers.cloudflare.com/workers/runtime-apis/cache/)

See [Cache](../services/cache.md) for the full behavior, freshness
rules, and limits.

- The cache is per-node and in-memory: entries survive across isolates of
  one script on one node, never cross scripts, and never cross nodes.
- `fetch()` neither reads nor populates the cache, and `cf.cacheKey` is
  unsupported; the request URL is the whole key.
- No API purges by `Cache-Tag`; purge by URL with `delete`.

### [TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)

- A socket cannot outlive its event. Your Durable Object must reconnect
  during a later event.
- Cello verifies a TLS server against its bundled Mozilla root store.
- Cello does not block the destination ports that Cloudflare blocks. Your
  fleet network controls the egress policy.

### BroadcastChannel

The class exists so that your bundle loads, but its constructor throws.

## Compatibility flags

Cello honors these compatibility flags:

- `delete_all_deletes_alarm`
- `js_rpc`
- `fetcher_no_get_put_delete`
- `sqlite_vec`
- `websocket_standard_binary_type`
- The static-assets navigation flags

Cello accepts every other flag without effect.
`Cloudflare.compatibilityFlags` reports only the honored flags.

## Wrangler configuration

`cello deploy` accepts `wrangler.jsonc` or `wrangler.json`, not
`wrangler.toml`.

Your `name` must contain 1 to 63 lowercase ASCII letters, digits, or
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
JavaScript expression, as in Wrangler. A rule `type` must be `Text`,
`Data`, or `CompiledWasm`, and each glob must have the form `**/*.ext`
or `*.ext`, because esbuild selects a loader by extension. Two rules that
give one extension different types stop the deployment. Cello already
applies `CompiledWasm` to `**/*.wasm`, so only that type is valid for
`.wasm`. `no_bundle` skips esbuild, so `no_bundle` with `define` or
`rules` stops the deployment.

An asset-only project can omit `main`. `cello deploy` refuses an unsafe
asset path, and `.assetsignore` requires Wrangler.

See [Limitations](limits.md) for the operational limits.
