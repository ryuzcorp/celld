<!-- Modified by Ryuz Corp (Cello): Workers Cache API page; deployment cache scope. -->
---
title: Cache
description: Cache responses in memory on each node with the Workers Cache API.
---

The [Workers Cache API](https://developers.cloudflare.com/workers/runtime-apis/cache/) is a per-node in-memory cache: `caches.default` plus one namespace per `caches.open(name)`. There is no binding to declare; `caches` is a global.

## Run a minimal example

Declare nothing. A response one request stores is visible to the next request on the same node.

The full runnable version is in [`examples/cache`](https://github.com/ryuzcorp/cello/tree/main/examples/cache).

<!-- cello-example: cache -->

```js
export default {
  async fetch(request) {
    const cache = caches.default;
    const hit = await cache.match(request);
    if (hit) return hit;
    const response = new Response("computed at " + Date.now(), {
      headers: { "Cache-Control": "max-age=60" },
    });
    await cache.put(request, response.clone());
    return response;
  },
};
```

By default no script reads another script's entries. Set `CELLO_CACHE_SCOPE=deployment` to share the cache across the deployment instead (see below).

## Use the supported API

- `caches.default` is the default namespace. `caches.open(name)` returns the named namespace, creating it on first use. `open("default")` is a different namespace from `caches.default`.
- `put(request, response)` stores the response under the request URL when the response is cacheable, and resolves `undefined` whether it stored or not. It consumes the response body, and it snapshots the status, headers, and body. It throws for a non-GET request, a 206 response, or a `Vary: *` response, as Cloudflare's does.
- `match(request, { ignoreMethod })` answers the stored response, or `undefined` on a miss. A conditional request with a matching `If-None-Match` or a covering `If-Modified-Since` gets a 304 with no body; a satisfiable `Range` gets a 206 slice (or a multipart body for several ranges); an unsatisfiable `Range` gets a 416. Every answer built from a stored entry carries `CF-Cache-Status: HIT`, and no answer carries `Cache-Tag`. Each `match` builds a fresh response, so two reads never share a body.
- `delete(request, { ignoreMethod })` purges one entry and reports whether one was there. Like Cloudflare's, it purges the cache the Worker runs against — this node's — and no other.

## Control freshness

Freshness follows Cloudflare, highest precedence first: `s-maxage`, then `max-age`, then `Expires`. `Age` and `Date` discount the lifetime, as at the edge. When no freshness header is present, the edge-TTL defaults apply:

| Status | Default TTL |
| --- | --- |
| 200, 301 | 120 minutes |
| 302, 303 | 20 minutes |
| 404, 410 | 3 minutes |

Any other status needs explicit freshness to be stored. An entry becomes invisible the moment it expires.

These responses are never cached: `no-store`, `private`, `no-cache`, and any response with a `Set-Cookie` header. `no-cache` is not stored because `match` never revalidates against an origin. The one documented exception holds: `Cache-Control: private=set-cookie` stores the response with its `Set-Cookie` headers removed.

`Vary` is ignored — one entry answers every variant — except that `put` throws for `Vary: *`. Request `Cache-Control` is ignored. Answers carry no `Age` header.

## Cache scope

`CELLO_CACHE_SCOPE` selects whose entries a Worker sees. `script` (the default) keeps one script's entries apart from every other script's. `deployment` shares one `caches.default` and one set of `caches.open(name)` namespaces across every script of the serving deployment — the primary script plus every co-hosted script — like one Cloudflare zone.

The scope is a process-wide operator setting from the node's environment at startup. Workers cannot read or change it. An unknown or malformed value fails closed: the node refuses to start rather than silently sharing entries across scripts.

Sharing is total: in deployment scope every script of the deployment can read, overwrite, and purge every other script's entries. See [security](../guides/security.md#cache-scope-and-co-hosted-scripts). A different deployment — a redeploy with a new version, or another fleet — starts empty and never sees these entries; the old scope drops once its last serving generation drains. A forced reload of the same deployment keeps its entries.

The store is process-local per node: entries never leave the node that wrote them. Isolates outside any deployment share nothing. Each scope carries its own `CELLO_CACHE_MAX_BYTES` budget and recency: in deployment scope one deployment's writes never evict another's.

## Handle the differences from Cloudflare

| Area | Cello behavior |
| --- | --- |
| Scope | Node-local and best-effort. Each node caches independently, and a `delete` reaches only the node that ran it. |
| `fetch()` integration | None. Cello has no `cf` cache options, and the default namespace stands alone instead of sharing with `fetch`. |
| Purge by tag | Unsupported. Tags are accepted on `put` and stripped on `match`; purge by URL with `delete`. |
| Custom cache keys | Unsupported (`cf.cacheKey`); the request URL is the whole key. |

## Operate it

- `CELLO_CACHE_MAX_BYTES` caps each cache scope at 64 MiB by default; `0` disables the cache, and a malformed value refuses the boot. One entry never exceeds 16 MiB, body plus headers plus key. Past the cap the least-recently-used entries go first; a `match` counts as use, so hot entries stay.
- The cache is not persisted: a restart drops it. In script scope a deployment keeps a script's entries, as cached responses outlive deploys at Cloudflare too — version the URLs or `delete` to purge. In deployment scope a new deployment starts empty instead.
- Scripts share the one node budget with no per-script partition, so one script's writes can evict another's. Size the cap for the sum of the node's scripts.
- When memory pressure starts shedding load (`CELLO_MAX_RSS_MB`), the node drops the whole cache before shedding cells. Puts keep working while shedding; only entering pressure clears.
