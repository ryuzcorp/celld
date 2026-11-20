---
title: Durable Object Facets
description: Run child objects with their own databases inside one Durable Object cell.
---

A facet is a child object inside a Durable Object, with a SQLite database of its own. A supervisor class starts a facet from a class that a [Worker Loader](dynamic-workers.md) provides, so generated or untrusted code gets durable storage without a Durable Object namespace. Read the [Cloudflare Durable Object Facets documentation](https://developers.cloudflare.com/dynamic-workers/usage/durable-object-facets/) for the standard API.

## Run a minimal example

The supervisor below loads the `App` class from an inline module and starts one facet named `"app"`. External traffic reaches the facet only through the supervisor.

```jsonc
// wrangler.jsonc
{
  "name": "facets",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "worker_loaders": [{ "binding": "LOADER" }],
  "durable_objects": {
    "bindings": [{ "name": "SUPERVISOR", "class_name": "Supervisor" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Supervisor"] }]
}
```

```js
// index.js (supervisor; the facet class arrives through the loader)
import { DurableObject } from "cloudflare:workers";

export class Supervisor extends DurableObject {
  async fetch(request) {
    const worker = this.env.LOADER.get("app-v1", () => ({
      compatibilityDate: "2025-01-01",
      mainModule: "app.js",
      modules: { "app.js": "export class App extends DurableObject { /* ... */ }" },
    }));
    const facet = this.ctx.facets.get("app", () => ({
      class: worker.getDurableObjectClass("App"),
    }));
    return facet.fetch(request);
  }
}

export default {
  async fetch(request, env) {
    return env.SUPERVISOR.getByName("only").fetch(request);
  },
};
```

## Use the supported API

- `ctx.facets.get(name, callback)` returns a stub for the named facet. The callback returns `{ class, id }`, and Cello runs it only to start the facet.
- `worker.getDurableObjectClass("App")` on a Worker Loader binding, or `ctx.exports.App`, supplies the `class`.
- `ctx.exports.App({ props })` creates a class handle with startup properties.
- `ctx.facets.abort(name, reason)` stops a facet and keeps its database.
- `ctx.facets.delete(name)` stops a facet and removes its database.
- The stub answers `fetch()` and the RPC methods of the class.

## Share the root cell

A facet is not a cell. It has no ownership record and no fencing epoch of its own; it uses those of the root cell, which the [Durable Objects page](durable-objects.md#rely-on-one-owner-at-a-time) describes.

A facet runs on the owner node of the root cell. An eviction, a reset, and a move take the root object and its facets as one group, and the startup callback runs again at the next call.

As in workerd, a facet write commits in the database of the facet, so rolling back a root transaction does not undo a facet call inside it. A facet and its root object never commit atomically. Cello holds an outbound effect of a facet, and the reply of a facet call, until the facet stream proves the writes of that call. The effect also passes the output gate of the root cell.

## Start and address a facet

Cello runs the `ctx.facets.get(name, callback)` callback only when the facet is not already running. The `class` comes from `worker.getDurableObjectClass("App")` on a Worker Loader binding, or from `ctx.exports.App` for an exported `DurableObject` class without a storage migration; that facet runs in the isolate of its root.

A loaded class can extend `DurableObject`, or it can be a plain class with a `(state, env)` constructor. A plain class answers `fetch()`, and its RPC methods require the `js_rpc` compatibility flag, as on workerd.

`id` sets `ctx.id` in the facet. A `DurableObjectId` keeps its name, a string ID stays a string, and an omitted ID inherits the parent ID and its name. An unmigrated class handle has no `idFromName()`, so use a Durable Object namespace to create a named ID.

`ctx.exports.App({ props })` creates a class handle with startup properties. The call copies the properties, so a later change does not reach `ctx.props`. The properties must support structured cloning.

- The name alone selects the database, so a changed `id` still reaches the stored data. A name has a 256-byte limit.
- A facet can start facets of its own, to a total depth of 4 counting the root object.
- `ctx.facets.abort(name, reason)` stops a facet and keeps its database. A call on the stopped stub throws the reason.
- `ctx.facets.delete(name)` stops the facet and removes its database and the database of every facet below it.

No name reaches a facet from outside its root object. Use a separate Durable Object, addressed with [`idFromName()`](durable-objects.md#address-a-cell), when children must run on different nodes.

## Use WebSockets in a facet

A facet accepts a WebSocket when its `fetch()` returns a `101` response with the client end of a `WebSocketPair`, and the root object returns that response. The facet calls `accept()` on the server end or passes it to `ctx.acceptWebSocket()`. A facet also opens a WebSocket with `new WebSocket(url)` or an `Upgrade: websocket` fetch. Cello delivers each socket's events to the facet, not the root object. Socket output waits for the facet stream and passes the output gate of the root cell.

A facet socket does not hibernate, because Cello cannot start a facet again without its root object. A socket the facet accepts or opens keeps the root cell resident until it closes. `getWebSockets()` returns the sockets of that facet.

`ctx.facets.abort()` and `ctx.facets.delete()` close each stopped facet's sockets with code `1001`, and the facet receives no close event. workerd drops the connection instead, so its client sees `1006`. A drain, an ownership move, or a generation swap closes a facet socket with `1012`, and an output gate failure closes it with `1011`.

## Handle the differences from Cloudflare

| Area | Cello behavior |
| --- | --- |
| Class source | A Durable Object binding cannot supply a facet class. A class with a storage migration cannot either, because `ctx.exports` holds that class as a namespace. |
| Storage | Each facet has a separate SQLite database, replicated as a separate stream under the bucket prefix of the root object. |
| Alarms | A facet cannot set one. `storage.setAlarm()` throws inside a facet, so the root object holds the schedule. |
| Sockets | A facet socket does not hibernate. It keeps the root cell resident until it closes. |
| Stubs | A facet stub is not awaitable, and no pipelined property path exists, so a call must name one method. `clone()` is unavailable. |

The [Cloudflare compatibility](../reference/cloudflare-compat.md#services) page lists the runtime APIs and the unsupported services.

## Operate it

- `cello dev .` runs the supervisor and its facets locally. Facet state lives with the root cell's local state in `.cello/dev`.
- A facet never leaves its root cell's node. When children must run on different nodes, use separate Durable Objects instead.
