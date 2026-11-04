// Modified by ryuzcorp (ryuzcorp/celld): deployment cache scope fixture. Apache-2.0.

// Every endpoint drives `caches` and reports JSON. Cache keys are absolute
// URLs under a fake origin; nothing is ever fetched. MARKER names the
// deployed revision: the redeploy test boots v1, deploys a copy stamped v2,
// and polls `marker` until the new deployment answers.
const MARKER = 'v1';
const KEY_ORIGIN = 'https://scope.test/';

const cacheFor = (name) =>
  name !== null && name !== undefined && name !== ''
    ? caches.open(name)
    : caches.default;

const keyUrl = (key) => KEY_ORIGIN + key;

async function doPut(q) {
  const key = q.get('key');
  const body = q.get('body') ?? `body:${key}`;
  const cc = q.get('cc') ?? 'max-age=120';
  const cache = await cacheFor(q.get('cache'));
  const response = new Response(body, { headers: { 'cache-control': cc } });
  await cache.put(new Request(keyUrl(key)), response);
  return { bodyUsed: response.bodyUsed };
}

async function doMatch(q) {
  const cache = await cacheFor(q.get('cache'));
  const found = await cache.match(new Request(keyUrl(q.get('key'))));
  if (found === undefined) return { hit: false };
  return { hit: true, status: found.status, body: await found.text() };
}

async function doDelete(q) {
  const cache = await cacheFor(q.get('cache'));
  return { deleted: await cache.delete(new Request(keyUrl(q.get('key')))) };
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const t = url.pathname.slice(1);
    const q = url.searchParams;
    const run = async () => {
      switch (t) {
        case 'health':
          return {};
        case 'marker':
          return { marker: MARKER };
        case 'put':
          return doPut(q);
        case 'match':
          return doMatch(q);
        case 'delete':
          return doDelete(q);
        case 'peer-check': {
          const key = q.get('key');
          const cache = q.get('cache') ?? '';
          return { peerHit: await env.SB.check(cache, keyUrl(key)) };
        }
        case 'peer-get': {
          const key = q.get('key');
          const cache = q.get('cache') ?? '';
          return await env.SB.get(cache, keyUrl(key));
        }
        case 'peer-put': {
          const key = q.get('key');
          const cache = q.get('cache') ?? '';
          const body = q.get('body') ?? `probe:${key}`;
          await env.SB.put(cache, keyUrl(key), body);
          return {};
        }
        case 'peer-delete': {
          const key = q.get('key');
          const cache = q.get('cache') ?? '';
          return { peerDeleted: await env.SB.del(cache, keyUrl(key)) };
        }
        default:
          throw new Error(`unknown endpoint ${t}`);
      }
    };
    try {
      return Response.json({ ok: true, result: await run() });
    } catch (e) {
      return Response.json({
        ok: false,
        name: e?.name,
        error: String((e && e.message) || e),
      });
    }
  },
};
