// Modified by ryuzcorp (ryuzcorp/celld): per-node Cache API store.
// The Workers Cache API over a per-node in-memory store.
//
// `caches.default` and every `caches.open(name)` namespace share one
// process-local store, so all isolates of a script on the node see the
// same entries, while no script can name another script's: the host keys
// every entry by the script name it read from isolate state. The store
// mirrors Cloudflare's documented Cache semantics -- freshness from
// `s-maxage` / `max-age` / `Expires`, edge-TTL defaults by status,
// `no-store` / `private` / `Set-Cookie` never cached, `Vary` ignored,
// conditional and range answers -- and every answer built from a stored
// entry carries `CF-Cache-Status: HIT`. See docs/services/cache.md.
(function () {
  const kCreate = Symbol("celld.Cache.create");

  // A key is a Request or URL string, and it must parse as an absolute
  // http(s) URL -- the same rule and message as Workerd's validateUrl.
  const asKey = (request) => {
    const url = request instanceof Request ? request.url : String(request);
    let parsed = null;
    try {
      parsed = new URL(url);
    } catch { /* rejected below */ }
    if (!parsed ||
      (parsed.protocol !== "http:" && parsed.protocol !== "https:")) {
      throw new TypeError(
        "Invalid URL. Cache API keys must be fully-qualified, valid URLs.");
    }
    return request instanceof Request ? request : new Request(url);
  };

  // The verbatim header list, one pair per append: casing and multiplicity
  // survive the round trip, and `set-cookie` values stay apart.
  const headerPairs = (headers) => headers.__celldHeaderList;

  // Largest single entry, body plus headers plus key: the budget the host
  // enforces in worker_cache.rs (`MAX_ENTRY_BYTES`). The drain below stops
  // at the same budget, so an over-cap body never reaches memory in full
  // before the refusal.
  const MAX_ENTRY_BYTES = 16 * 1024 * 1024;

  // Drain a response body with an explicit byte budget, like the KV value
  // drain: chunk by chunk, aborting as soon as the body alone exceeds the
  // per-entry cap. A body past the cap can never store -- the entry
  // only adds key and header bytes -- so the abort settles exactly as
  // buffering the whole body and letting the host refuse it would: the
  // producer sees
  // cancel, the body is consumed, and put() resolves undefined without
  // storing. Answers the bytes, or null past the cap.
  const drainCapped = async (response) => {
    // Already-buffered bodies never touch the stream: the bytes exist, so
    // arrayBuffer() can neither hang nor grow the heap.
    if (response._bodyBytes !== null || response.body === null) {
      const buffered = new Uint8Array(await response.arrayBuffer());
      return buffered.byteLength > MAX_ENTRY_BYTES ? null : buffered;
    }
    response.bodyUsed = true;
    const reader = response.body.getReader();
    const chunks = [];
    let size = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > MAX_ENTRY_BYTES) {
          // Over cap: stop pulling and release the producer. The reader
          // stays locked, as after __drainBody, so the consumed body can
          // be neither re-read nor cloned.
          await reader.cancel(new Error(
            "cache.put: response body exceeds the 16 MiB entry limit"));
          return null;
        }
        chunks.push(value);
      }
    } catch (error) {
      await reader.cancel(error).catch(() => {});
      throw error;
    }
    const body = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    // Memoize, as __drainBody does: later reads of the put response see
    // the same bytes.
    response._bodyBytes = body;
    response._body = undefined;
    return body;
  };

  class Cache {
    constructor(token = undefined, namespace = "default") {
      if (token !== kCreate) throw new TypeError("Illegal constructor");
      this._namespace = namespace;
    }

    get [Symbol.toStringTag]() { return "Cache"; }

    async put(request, response) {
      const key = asKey(request);
      if (!(response instanceof Response)) {
        throw new TypeError(
          "Cache.put() requires a Response as its second argument.");
      }
      if (key.method !== "GET") {
        throw new TypeError("Cannot cache response to non-GET request.");
      }
      if (response.status === 206) {
        throw new TypeError(
          "Cannot cache response to a range request (206 Partial Content).");
      }
      const vary = response.headers.get("vary");
      if (vary !== null && vary.includes("*")) {
        throw new TypeError("Cannot cache response with 'Vary: *' header.");
      }
      if (response.bodyUsed) {
        throw new TypeError(
          "Cannot cache a response whose body is already used.");
      }
      // Buffering disturbs the response, which is put()'s contract, and
      // hands the host bytes it owns: later reads or mutations of the
      // original can never reach the stored copy. The drain aborts past
      // the per-entry cap, settling like the host's own refusal.
      const body = await drainCapped(response);
      if (body === null) return;
      // The host answers whether the response stuck -- directives,
      // staleness, or size can refuse it -- but put() resolves undefined
      // either way, as Cloudflare's does.
      __cache_put(
        this._namespace,
        key.url,
        key.method,
        response.status,
        JSON.stringify(headerPairs(response.headers)),
        body,
      );
    }

    async match(request, options = undefined) {
      const key = asKey(request);
      const ignoreMethod = options?.ignoreMethod === true;
      const found = __cache_match(
        this._namespace,
        key.url,
        key.method,
        ignoreMethod,
        JSON.stringify(headerPairs(key.headers)),
      );
      if (found === null || found === undefined) return undefined;
      const [status, headersJson, body] = found;
      const headers = new Headers(JSON.parse(headersJson));
      // A null body is a 304, a 416, or a stored null-body status: those
      // construct with null, never with an empty byte string.
      if (body === null) return new Response(null, { status, headers });
      return new Response(body, { status, headers });
    }

    async delete(request, options = undefined) {
      const key = asKey(request);
      const ignoreMethod = options?.ignoreMethod === true;
      return __cache_delete(
        this._namespace,
        key.url,
        key.method,
        ignoreMethod,
      );
    }
  }

  class CacheStorage {
    constructor(token = undefined) {
      if (token !== kCreate) throw new TypeError("Illegal constructor");
    }

    get [Symbol.toStringTag]() { return "CacheStorage"; }

    async open(cacheName) {
      if (String(cacheName).length >= 1024) {
        throw new TypeError("Cache name is too long.");
      }
      return new Cache(kCreate, `named:${cacheName}`);
    }
  }

  const storage = new CacheStorage(kCreate);
  Object.defineProperty(storage, "default", {
    value: new Cache(kCreate, "default"),
    enumerable: true,
  });

  globalThis.Cache = Cache;
  globalThis.CacheStorage = CacheStorage;
  globalThis.caches = storage;
})();
