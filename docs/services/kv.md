---
title: KV
description: Store keys and values in a namespace cell, and manage them from the CLI.
---

KV is a key-value store that a Worker reaches through a binding. Each namespace is one Cello cell, and a value above 1 MiB lives in the fleet bucket. Read the [Cloudflare KV documentation](https://developers.cloudflare.com/kv/api/) for the API.

## Run a minimal example

This namespace stores one value per key: `PUT` writes the request body, `GET` reads it back, `DELETE` removes it. It runs unchanged on `cello dev` (verified with cello 0.2.1).

The full runnable version is in [`examples/kv`](https://github.com/ryuzcorp/cello/tree/main/examples/kv).

<!-- cello-example: kv -->

```jsonc
// wrangler.jsonc
{
  "name": "kv",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "kv_namespaces": [{ "binding": "VALUES", "id": "example-values" }]
}
```

```js
// index.js
export default {
  async fetch(request, env) {
    const key = new URL(request.url).pathname.slice(1);
    if (!key) return new Response("Use /KEY.", { status: 400 });
    if (request.method === "PUT") {
      await env.VALUES.put(key, request.body);
      return new Response(null, { status: 204 });
    }
    if (request.method === "DELETE") {
      await env.VALUES.delete(key);
      return new Response(null, { status: 204 });
    }
    const value = await env.VALUES.get(key);
    return value === null
      ? new Response("Not found.", { status: 404 })
      : new Response(value);
  },
};
```

```sh
cello dev .
curl -X PUT "http://127.0.0.1:9876/greet" --data-binary 'hello-kv'
curl "http://127.0.0.1:9876/greet"   # hello-kv
```

## Use the supported API

- `get(key, type)` returns the value as `"text"`, `"json"`, `"arrayBuffer"`, or `"stream"`. A missing key gives `null`.
- `getWithMetadata(key, type)` also returns the metadata of the write.
- `put(key, value, options)` takes a string, an `ArrayBuffer`, a typed array, or a `ReadableStream`. The options are `expiration` (absolute time in seconds), `expirationTtl` (seconds from now), and `metadata`.
- `delete(key)` removes a key.
- `list({ prefix, cursor })` returns a page of key names and a cursor for the next page.

A `kv_namespaces` entry gives a `binding` and an `id`. The `id` is the namespace identity, and it can be any string, such as a Cloudflare hexadecimal id or `sessions`. Two Workers that name one `id` reach one namespace. Cello ignores `preview_id`.

`put()` reads a `ReadableStream` to its end before the write, so `put(key, request.body)` works. `list()` returns at most 1000 keys in byte order, and a concurrent write cannot make the cursor skip a key.

Cello enforces the [Cloudflare KV limits](https://developers.cloudflare.com/kv/platform/limits/): a key of at most 512 bytes, a value of at most 25 MiB, metadata of at most 1024 bytes, and at most 100 keys in one bulk `get()`. A call that crosses a limit fails, and Cello never truncates data. The minimum expiration is 60 seconds. An expired key becomes invisible the moment it expires.

Every call goes to the node that owns the namespace cell, so a read costs one cell dispatch. Hold a value in a local variable when one request reads it many times. Writes to one namespace run one at a time, so use a Durable Object for a write-hot value such as a counter.

## Handle the differences from Cloudflare

| Area | Cello behavior |
| --- | --- |
| Consistency | Strong. A read reaches the cell that owns the namespace, so it never returns a stale value. Cloudflare KV is eventually consistent. |
| Edge cache | None. `cacheTtl` has no effect, and `cacheStatus` is `null`. |
| Large values | A value above 1 MiB requires a fleet bucket. Without one the write fails with `KV large values need a fleet bucket`. |
| Write capacity | One writer per namespace. Use more namespaces to increase write capacity. |

The [Cloudflare compatibility](../reference/cloudflare-compat.md#services) page lists the runtime APIs and the unsupported services.

## Operate it

- `cello kv get|put|delete|list|info NAMESPACE --bucket s3://NAME` reads and writes a deployed namespace. The namespace id is the `id` from the project's `kv_namespaces` entry, verbatim. `get` writes the value to stdout as bytes.
- `cello kv bulk get|put|delete NAMESPACE FILE --bucket s3://NAME` moves many keys at once. `put` accepts `--path FILE`, `--ttl SECONDS` (at least 60), `--expiration SECONDS`, and `--metadata JSON`.
- `cello dev .` serves the namespace from local state; no bucket is needed.
