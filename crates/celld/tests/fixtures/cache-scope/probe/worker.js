// Modified by ryuzcorp (ryuzcorp/celld): deployment cache scope fixture. Apache-2.0.
import { WorkerEntrypoint } from 'cloudflare:workers';

// The second script on the node. It answers over the service binding, so
// the app can ask what a *different script* sees in its own cache. An
// empty cache name addresses `caches.default`, any other name the matching
// `caches.open(name)` namespace.
const cacheFor = (name) =>
  name !== null && name !== undefined && name !== ''
    ? caches.open(name)
    : caches.default;

export default class ProbeService extends WorkerEntrypoint {
  async check(cache, url) {
    return (await (await cacheFor(cache)).match(url)) !== undefined;
  }
  async get(cache, url) {
    const found = await (await cacheFor(cache)).match(url);
    if (found === undefined) return { hit: false };
    return { hit: true, status: found.status, body: await found.text() };
  }
  async put(cache, url, body) {
    await (await cacheFor(cache)).put(
      url,
      new Response(body, { headers: { 'cache-control': 'max-age=120' } }),
    );
    return true;
  }
  async del(cache, url) {
    return await (await cacheFor(cache)).delete(url);
  }
}
