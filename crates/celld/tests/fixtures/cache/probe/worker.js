// Modified by ryuzcorp (ryuzcorp/celld): per-node Cache API fixture. Apache-2.0.
import { WorkerEntrypoint } from 'cloudflare:workers';

// The second script on the node. It answers over the service binding,
// so the app can ask what a *different script* sees in its own cache.
export default class ProbeService extends WorkerEntrypoint {
  async check(url) {
    return (await caches.default.match(url)) !== undefined;
  }
  async get(url) {
    const found = await caches.default.match(url);
    if (found === undefined) return { hit: false };
    return { hit: true, status: found.status, body: await found.text() };
  }
  async put(url, body) {
    await caches.default.put(
      url,
      new Response(body, { headers: { 'cache-control': 'max-age=120' } }),
    );
    return true;
  }
}
