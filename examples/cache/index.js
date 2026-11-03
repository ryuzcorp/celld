// Modified by ryuzcorp (ryuzcorp/celld): Workers Cache API example. Apache-2.0.
export default {
  async fetch(request) {
    const pathname = new URL(request.url).pathname;
    const answer = () =>
      new Response(`Expensive answer for ${pathname} at ${new Date().toISOString()}\n`, {
        headers: { "cache-control": "max-age=60", "content-type": "text/plain" },
      });
    // Only GET responses are cacheable; anything else skips the cache.
    if (request.method !== "GET") return answer();

    // A miss computes the answer once and stores it for a minute; a hit
    // serves the stored response without running the logic above.
    const cached = await caches.default.match(request);
    if (cached !== undefined) return cached;

    const response = answer();
    // put() consumes the body it is given, so the clone goes to the cache
    // and the original goes to the client.
    await caches.default.put(request, response.clone());
    return response;
  },
};
