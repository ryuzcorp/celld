---
title: What Cello isolates
description: One fleet runs one trusted application; the boundaries Cello draws and the ones you must draw.
---

One fleet runs one application. Cello trusts the application code, the fleet nodes, and the operators. Application code can use its configured bindings and can consume shared node resources. Do not run code from mutually distrusting tenants in one fleet.

Application code cannot reach the engine's host functions. An internal script receives them as function parameters, and `globalThis` carries no `__`-prefixed property. A Worker Loader worker with `globalOutbound: null` therefore reaches the host only through the capabilities in its `env`.

Cello depends on two external boundaries: a trusted private network protects the internal listener, and object storage credentials control the fleet.

## Isolate runtime cells

Cells of the shared runtime classes — D1 databases, KV namespaces, and Queue brokers — serve every script in the fleet, so they run in a dedicated runtime isolate pool whose isolates evaluate no tenant bundle: the harness plus a fixed inert entry module. No script's top level shares a realm with them, and the pool holds no tenant bindings, vars, services, loaders, or schedule — only the deployment-wide queue catalog, the node's routing identity, the generation tag, and the union of every declaring script's compatibility switches. Workflow cells and the cron cell stay in their own script's pool: they execute that script's `run()` and `scheduled()` handlers with its bindings, so isolating them from their own script would isolate them from the code they run. Per-cell placement and ownership are unchanged.

## Scope the cache

The Workers Cache API is per script by default: no script can read, overwrite, or purge another script's entries. `CELLO_CACHE_SCOPE=deployment` lifts that boundary for scripts of one deployment, like one Cloudflare zone: every script of the serving deployment shares one `caches.default` and one set of `caches.open(name)` namespaces, so a co-hosted script can read entries holding another script's session or personalized bodies, overwrite them, and purge them. Turn it on only when every script of the deployment is mutually trusted. The scope is an operator setting read from the node's process environment at startup; no binding, Worker var, or JS API can change it, and an unknown value refuses the boot rather than silently sharing. Sharing stays bounded: a different deployment or fleet still sees nothing, the runtime pool and dynamically loaded workers keep their own entries, and each scope carries its own size cap and recency (see [cache](../services/cache.md#cache-scope)).

## Fence cells without isolating tenants

Each cell is a SQLite database with one writer. An ownership epoch fences each cell, so a node that loses its lease cannot modify the current state. This fencing protects storage consistency. It does not isolate hostile applications. See [what Cello guarantees](durability.md) and [limits](../reference/limits.md).
