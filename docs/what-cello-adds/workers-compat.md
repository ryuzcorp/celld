---
title: Workers compatibility
description: Closer Cloudflare parity — cache, containers, crypto, SQL, and runtime APIs over upstream denoland/celld.
---

Patches that move Workers, Durable Objects, and the surrounding APIs
closer to Cloudflare. Binding and socket isolation is a hardening story
and lives on the [Security](security-hardening.md) page. Each entry
names its fork patch number.

## Cache and runtime cells

- `0018` — Per-node Workers Cache API: `caches.default` and
  `caches.open(name)` over a process-local store keyed by the script
  name, so all isolates of a script on the node share entries and no
  script can name another's. Cloudflare freshness (`s-maxage` /
  `max-age` / `Expires`, edge-TTL defaults), `no-store` / `private` /
  `Set-Cookie` never cached, `Vary` ignored, conditional and range
  answers, `CF-Cache-Status: HIT` on hits, and a `CELLO_CACHE_MAX_BYTES`
  cap (64 MiB default) with LRU eviction.
- `0045` — `CELLO_CACHE_SCOPE=deployment` shares one Workers Cache API
  scope across every script of the serving deployment — one
  `caches.default` and one set of `caches.open(name)` namespaces, like
  one Cloudflare zone. The default `script` scope keeps per-script
  entries byte-identical; the scope key renders from the fleet's bucket
  identity plus the serving deployment's identity, comes only from the
  node's process environment, and an unknown value refuses the boot.
  Sharing is total within a deployment; a redeploy starts empty.
- `0037` — Shared runtime cells run in a dedicated runtime isolate
  pool: cells of `__D1Database`, `__KvNamespace`, and `__Queue` build
  isolates from the harness plus a fixed inert entry module, never the
  declaring script's bundle, and hold no tenant bindings, vars,
  services, loaders, or schedule. Per-cell placement and ownership are
  unchanged — state stays in the cells' SQLite, so a rolling upgrade
  moves cells with no migration — and `/state` reports the pool under
  its reserved name `__cello-runtime`. Deploy refuses a service binding
  whose target is not a valid script name.

## Containers

- `0001` — `ctx.container.images` plus `start({ image })` select the
  image per container, with `images.default` for callers that read it.

## Runtime APIs

- `0109` — Under `nodejs_compat`, the Worker's `vars` populate
  `process.env` before the module evaluates (host value wins);
  `nodejs_compat_do_not_populate_process_env` opts back out. A
  dynamically loaded Worker's loader-supplied strings reach the
  handler's `env` and `process.env` alike, while the host-owned
  `CELLO_NODE` name never crosses into `process.env`. `__proto__`,
  `constructor`, and `prototype` never cross
  ([upstream #296](https://github.com/denoland/celld/issues/296)).
- `0110` — `SubtleCrypto` gains AES-KW wrap/unwrap plus symmetric JWK
  import/export; a large JWK export builds in 32 KiB chunks
  ([upstream #295](https://github.com/denoland/celld/issues/295)).
- `0111` — `SubtleCrypto` imports and exports EC public keys in
  uncompressed raw point form
  ([upstream #271](https://github.com/denoland/celld/issues/271)).
- `0112` — Writable request `/tmp` files (`node:fs` over a per-request
  dir) and `node:url` file-URL helpers, plus `appendFile` /
  `appendFileSync` / `fs.promises.appendFile` and `{ flag: "a" }`.
  New knob `CELLO_TMP_MAX_BYTES` (default 67108864, 64 MiB; validated
  positive): each entry costs `path.len() + 64` bytes against it, an
  oversize write is refused before its bytes copy, and stream chunks
  apply the cap as they arrive
  ([upstream #280](https://github.com/denoland/celld/issues/280)).
- `0113` — Workerd's sendable close codes: an endpoint may send
  1000–4999 except the reserved no-status codes 1004, 1005, 1006, and
  1015, which stay refused. Codes 1001–1014 that used to throw now
  close; a caller-initiated close is delivered on both pipes
  ([upstream #279](https://github.com/denoland/celld/issues/279)).
  Default behavior change.
- `0114` — `fetch()` responses with `Content-Encoding: gzip` or
  `deflate` decompress chunk by chunk; a truncated body is an error,
  not a clean end, and multi-member gzip concatenates
  ([upstream #238](https://github.com/denoland/celld/issues/238)).
- `0116` — `dir/index.html` serves at its trailing-slash URL, not only
  the directory URL
  ([upstream #318](https://github.com/denoland/celld/issues/318)).
- `0130` — Tenant TLS roots are shared across fetch, sockets, and
  outbound WebSockets: Mozilla's anchors plus the OS store and
  `SSL_CERT_FILE`, cached. A downloaded CA the OS trusts now works for
  tenant `fetch` too
  ([upstream #230](https://github.com/denoland/celld/issues/230)).
- `0131` — Host-injected `CELLO_NODE` identity: `env` carries the name
  of the node running the code, injected by the host after every
  deployment binding. The name is reserved — `vars`, `.dev.vars`, and
  every other binding kind are refused at deploy time, and a loaded
  Worker's props cannot smuggle it — while a `CELLO_NODE` entry in
  `.env` / `.env.local` is skipped with a warning and the host value
  wins at runtime
  ([upstream #219](https://github.com/denoland/celld/issues/219)). This
  changes deploy behavior for projects binding the name.
