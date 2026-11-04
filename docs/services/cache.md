<!-- Modified by ryuzcorp (ryuzcorp/celld): Workers Cache API page; deployment cache scope. -->
# Cache

The [Workers Cache API](https://developers.cloudflare.com/workers/runtime-apis/cache/)
is a per-node in-memory cache: `caches.default` plus one namespace per
`caches.open(name)`. Every isolate of a script on the node shares it, so a
response one request stores is visible to the next. By default no script
can read another script's entries; `CELLD_CACHE_SCOPE=deployment` shares
the cache across the deployment instead (see Cache scope). `caches` is a
global; there is no binding to declare. Read the
[Cloudflare Cache documentation](https://developers.cloudflare.com/workers/runtime-apis/cache/)
for the API.

## Example

The [Cache example](../../examples/cache) computes an answer once, stores
it for a minute, and serves the stored response while it is fresh.

<!-- celld-example: cache -->

## API

- `caches.default` is the default namespace. `caches.open(name)` returns
  the named namespace, creating it on first use. `open("default")` is a
  different namespace from `caches.default`.
- `put(request, response)` stores the response under the request URL when
  the response is cacheable, and resolves `undefined` whether it stored or
  not. It consumes the response body, and it snapshots the status, headers,
  and body, so later reads of the original cannot disturb the entry. It
  throws for a non-GET request, a 206 response, or a `Vary: *` response,
  as Cloudflare's does.
- `match(request, { ignoreMethod })` answers the stored response, or
  `undefined` on a miss. A conditional request with a matching
  `If-None-Match` or a covering `If-Modified-Since` gets a 304 with no
  body; a satisfiable `Range` gets a 206 slice (or a multipart body for
  several ranges); an unsatisfiable `Range` gets a 416. Every answer built
  from a stored entry carries `CF-Cache-Status: HIT`, and no answer carries
  `Cache-Tag`. Each `match` builds a fresh response, so two reads never
  share a body.
- `delete(request, { ignoreMethod })` purges one entry and reports whether
  one was there. Like Cloudflare's, it purges the cache the Worker runs
  against — this node's — and no other.

## Freshness

Freshness follows Cloudflare, highest precedence first: `s-maxage`, then
`max-age`, then `Expires`. `Age` and `Date` discount the lifetime, as at
the edge. When no freshness header is present, the edge-TTL defaults apply:

| Status | Default TTL |
| --- | --- |
| 200, 301 | 120 minutes |
| 302, 303 | 20 minutes |
| 404, 410 | 3 minutes |

Any other status needs explicit freshness to be stored. An entry becomes
invisible the moment it expires.

These responses are never cached: `no-store`, `private`, `no-cache`, and
any response with a `Set-Cookie` header. `no-cache` is not stored because
`match` never revalidates against an origin — serving a stored `no-cache`
entry would violate the directive, so it reads as a miss. The one
documented exception holds: `Cache-Control: private=set-cookie` stores the
response with its `Set-Cookie` headers removed.

`Vary` is ignored — one entry answers every variant — except that `put`
throws for `Vary: *`, as Cloudflare's does. Request `Cache-Control` is
ignored. Answers carry no `Age` header.

## Cache scope

`CELLD_CACHE_SCOPE` selects whose entries a Worker sees. `script` (the
default) keeps one script's entries apart from every other script's, as
above. `deployment` shares one `caches.default` and one set of
`caches.open(name)` namespaces across every script of the serving
deployment — the primary script plus every co-hosted script — like one
Cloudflare zone. Named namespaces scope exactly like the default
namespace: in deployment scope every script of the deployment shares
each name, and in script scope every name stays per script.

The scope is a process-wide operator setting taken from the node's
environment at startup. Workers cannot read or change it: no binding,
no Worker var, and no JS API names the scope, and the deployment key —
the fleet's bucket identity plus the serving deployment's versions —
is rendered by the host at build and stamped into tenant deployment
scripts' isolate state only. An unknown or malformed value fails
closed: the node refuses to start rather than silently sharing entries
across scripts.

Sharing is total: in deployment scope every script of the deployment can
read, overwrite, and purge every other script's entries, which is why the
scope is opt-in (see [security](../security.md#cache-scope-and-co-hosted-scripts)).
A different deployment — a redeploy with a new version, or another fleet —
starts with an empty scope and never sees these entries; the old scope is
dropped once its last serving generation drains. A forced reload of the
same deployment keeps its entries.

The store is process-local per node: entries never leave the node that
wrote them. The fleet's bucket identity (scheme, bucket name, and key
prefix) is part of the key anyway, so two fleets serving the same
versions in different buckets or prefixes never share entries even if
the store is ever shared beyond one process. The endpoint and storage
account are not part of it: one node serves one fleet from one
endpoint.

Isolates outside any deployment share nothing. The runtime pool that
hosts the shared D1, KV, and Queue cells runs no tenant fetch handlers
and keeps its own entries under its reserved name, as does each
dynamically loaded worker, which is not a script the deployment names.

Each scope carries its own `CELLD_CACHE_MAX_BYTES` budget and recency:
in deployment scope one deployment's writes never evict another's. In
script scope all scripts still share the one node budget below.

## Configuration and limits

`CELLD_CACHE_MAX_BYTES` caps each cache scope at 64 MiB by default;
`0` disables the cache, and a malformed value refuses the boot. One entry
can never exceed 16 MiB, body plus headers plus key. Past the cap the
least-recently-used entries go first; a `match` counts as use, so hot
entries stay. The cache is not persisted: a restart drops it, and in
script scope a deployment keeps a script's entries, as cached responses
outlive deploys at Cloudflare too — version the URLs or `delete` to
purge. In deployment scope a new deployment starts empty instead.

Scripts share the one node budget: there is no per-script partition or
quota, so one script's writes can evict another script's entries. This
is accepted, not limited — the node serves one fleet and the cache is
best-effort — so size the cap for the sum of the node's scripts.

When memory pressure starts shedding load (`CELLD_MAX_RSS_MB`), the
node drops the whole cache before shedding cells: best-effort bytes go
first. Puts keep working while shedding; only entering pressure clears.

## Differences from Cloudflare

- The cache is node-local and best-effort across nodes: each node caches
  independently, so a fleet behind a load balancer may hold the same URL on
  every node, and a `delete` reaches only the node that ran it.
- `fetch()` neither reads nor populates the cache: celld has no `cf` cache
  options, and the default namespace stands alone instead of being shared
  with `fetch` as Cloudflare's is.
- Tags are accepted on `put` and stripped on `match`, but no API purges by
  tag; purge by URL with `delete`.
- Custom cache keys (`cf.cacheKey`) are unsupported; the request URL is the
  whole key.
