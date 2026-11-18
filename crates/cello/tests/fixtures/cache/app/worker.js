// Modified by ryuzcorp (ryuzcorp/celld): per-node Cache API fixture. Apache-2.0.

// Every endpoint drives `caches` and reports JSON. Cache keys are absolute
// URLs under a fake origin; nothing is ever fetched.
const KEY_ORIGIN = 'https://cache.test/';

const cacheFor = (name) =>
  name !== null && name !== undefined && name !== ''
    ? caches.open(name)
    : caches.default;

const headersObject = (headers) => {
  const out = {};
  headers.forEach((value, name) => {
    out[name] = value;
  });
  return out;
};

const keyUrl = (key) => KEY_ORIGIN + key;

async function doPut(q) {
  const key = q.get('key');
  const headers = {};
  const passthrough = [
    ['cc', 'cache-control'],
    ['eta', 'etag'],
    ['lm', 'last-modified'],
    ['ct', 'content-type'],
    ['tag', 'cache-tag'],
    ['vary', 'vary'],
    ['expires', 'expires'],
    ['age', 'age'],
    ['date', 'date'],
    ['ccs', 'cf-cache-status'],
  ];
  for (const [param, name] of passthrough) {
    const value = q.get(param);
    if (value !== null) headers[name] = value;
  }
  const status = Number(q.get('status') ?? 200);
  const body = q.get('body') ?? `body:${key}`;
  const response = new Response(body, { status, headers });
  // Set-Cookie values stay apart: one append per value, never joined.
  for (const param of ['sc', 'sc2']) {
    const value = q.get(param);
    if (value !== null) response.headers.append('set-cookie', value);
  }
  const method = q.get('method') ?? 'GET';
  const request = new Request(keyUrl(key), { method });
  const cache = await cacheFor(q.get('cache'));
  await cache.put(request, response);
  return { bodyUsed: response.bodyUsed };
}

async function doMatch(q) {
  const headers = {};
  if (q.get('inm') !== null) headers['if-none-match'] = q.get('inm');
  if (q.get('ims') !== null) headers['if-modified-since'] = q.get('ims');
  if (q.get('range') !== null) headers['range'] = q.get('range');
  if (q.get('accept') !== null) headers['accept-encoding'] = q.get('accept');
  const request = new Request(keyUrl(q.get('key')), {
    method: q.get('method') ?? 'GET',
    headers,
  });
  const cache = await cacheFor(q.get('cache'));
  const options = q.get('ignore') === '1' ? { ignoreMethod: true } : undefined;
  const found = await cache.match(request, options);
  if (found === undefined) return { hit: false };
  return {
    hit: true,
    status: found.status,
    headers: headersObject(found.headers),
    body: await found.text(),
  };
}

async function doDelete(q) {
  const request = new Request(keyUrl(q.get('key')), {
    method: q.get('method') ?? 'GET',
  });
  const cache = await cacheFor(q.get('cache'));
  const options = q.get('ignore') === '1' ? { ignoreMethod: true } : undefined;
  return { deleted: await cache.delete(request, options) };
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
        case 'put':
          return doPut(q);
        case 'match':
          return doMatch(q);
        case 'delete':
          return doDelete(q);
        case 'vary-star': {
          try {
            await caches.default.put(
              keyUrl('vary-star'),
              new Response('x', { headers: { vary: '*' } }),
            );
            return { threw: false };
          } catch (e) {
            return { threw: true, name: e.name, message: e.message };
          }
        }
        case 'put-206': {
          try {
            await caches.default.put(keyUrl('p206'), new Response('x', { status: 206 }));
            return { threw: false };
          } catch (e) {
            return { threw: true, name: e.name, message: e.message };
          }
        }
        case 'put-post': {
          try {
            await caches.default.put(
              new Request(keyUrl('ppost'), { method: 'POST' }),
              new Response('x'),
            );
            return { threw: false };
          } catch (e) {
            return { threw: true, name: e.name, message: e.message };
          }
        }
        case 'put-used': {
          try {
            const res = new Response('x');
            await res.text();
            await caches.default.put(keyUrl('pused'), res);
            return { threw: false };
          } catch (e) {
            return { threw: true, name: e.name, message: e.message };
          }
        }
        case 'open-long': {
          try {
            await caches.open('n'.repeat(1024));
            return { threw: false };
          } catch (e) {
            return { threw: true, name: e.name };
          }
        }
        case 'put-error': {
          try {
            await caches.default.put(keyUrl('perr'), Response.error());
            return { threw: false };
          } catch (e) {
            return { threw: true, name: e.name, message: e.message };
          }
        }
        case 'oversize-stream': {
          // A body that never ends: put() must abort at the 16 MiB
          // per-entry cap instead of buffering forever. The producer
          // counts what the drain pulls, so the test asserts bounded
          // memory on bytes, not time.
          const CHUNK = 64 * 1024;
          const chunk = new Uint8Array(CHUNK);
          let pulled = 0;
          let cancelled = false;
          const stream = new ReadableStream({
            pull(controller) {
              pulled += CHUNK;
              controller.enqueue(chunk);
            },
            cancel() {
              cancelled = true;
            },
          });
          const response = new Response(stream, {
            headers: { 'cache-control': 'max-age=120' },
          });
          let threw = null;
          try {
            await caches.default.put(keyUrl('oversize-stream'), response);
          } catch (e) {
            threw = { name: e.name, message: e.message };
          }
          const found = await caches.default.match(
            keyUrl('oversize-stream'),
          );
          return {
            threw,
            bodyUsed: response.bodyUsed,
            pulled,
            cancelled,
            hit: found !== undefined,
          };
        }
        case 'stream-roundtrip': {
          // A finite stream under the cap still stores and reads back
          // whole: the cap aborts over-cap bodies, nothing else.
          const CHUNK = 64 * 1024;
          const total = 16 * CHUNK;
          let sent = 0;
          const stream = new ReadableStream({
            pull(controller) {
              if (sent >= total) {
                controller.close();
                return;
              }
              controller.enqueue(new Uint8Array(CHUNK).fill(7));
              sent += CHUNK;
            },
          });
          await caches.default.put(
            keyUrl('stream-roundtrip'),
            new Response(stream, {
              headers: { 'cache-control': 'max-age=120' },
            }),
          );
          const found = await caches.default.match(
            keyUrl('stream-roundtrip'),
          );
          if (found === undefined) return { hit: false };
          return { hit: true, length: (await found.text()).length };
        }
        case 'peer-check': {
          const key = q.get('key');
          return { peerHit: await env.SB.check(keyUrl(key)) };
        }
        case 'peer-get': {
          const key = q.get('key');
          return await env.SB.get(keyUrl(key));
        }
        case 'peer-put': {
          const key = q.get('key');
          const body = q.get('body') ?? `probe:${key}`;
          await env.SB.put(keyUrl(key), body);
          return {};
        }
        case 'evict':
        case 'evict-lru': {
          const keys = q.get('keys').split(',');
          const size = Number(q.get('size') ?? 1400);
          const body = 'v'.repeat(size);
          const putOne = async (key) => {
            await caches.default.put(
              keyUrl(key),
              new Response(body, {
                headers: {
                  'cache-control': 'max-age=120',
                  'content-type': 'text/plain',
                },
              }),
            );
          };
          const matchOne = async (key) => {
            const found = await caches.default.match(keyUrl(key));
            if (found === undefined) return { hit: false };
            return {
              hit: true,
              ccs: found.headers.get('cf-cache-status'),
              length: (await found.text()).length,
            };
          };
          if (t === 'evict') {
            for (const key of keys) await putOne(key);
          } else {
            // Touch the first key after the first two puts, so the
            // second key is least-recently-used when the third lands.
            await putOne(keys[0]);
            await putOne(keys[1]);
            await matchOne(keys[0]);
            await putOne(keys[2]);
          }
          const hits = {};
          for (const key of keys) hits[key] = await matchOne(key);
          return { hits };
        }
        case 'double-read': {
          const key = q.get('key');
          const first = await caches.default.match(keyUrl(key));
          const b1 = await first.text();
          const second = await caches.default.match(keyUrl(key));
          const b2 = await second.text();
          return { b1, b2 };
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
