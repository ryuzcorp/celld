// Modified by ryuzcorp (ryuzcorp/celld): runtime-isolate victim fixture; Queue pollution-gate probes. Apache-2.0.
// The second script in the fleet: holds D1/KV/Queue bindings and probes both
// their data paths and the operator-only surface. Its own top level is clean;
// the first script's top level overrides shared primordials, so every probe
// here reads a shared runtime cell that must be unaffected by that pollution.
const J = (o, st = 200) => Response.json(o, { status: st });
const MARK = "__celld_probe_marker";

const stubPost = (stub, body) =>
  stub.fetch("http://cell/", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

const rpcProbe = async (call) => {
  try {
    await call();
    return { threw: false };
  } catch (e) { return { threw: true, name: e.name, message: String(e.message || e) }; }
};

const d1seed = async (env) => {
  await env.DB.exec(
    "CREATE TABLE IF NOT EXISTS gate (id INTEGER PRIMARY KEY, v TEXT); " +
    "DELETE FROM gate; " +
    "INSERT INTO gate VALUES (1, 'one'), (2, 'two'); " +
    "CREATE TABLE IF NOT EXISTS consumed (id TEXT); " +
    "DELETE FROM consumed;"
  );
  return { seeded: true };
};

const handlers = {
  'health': async () => ({ ok: true }),
  'seed': d1seed,
  'd1-binding-ok': async (env) => {
    const one = await env.DB.prepare("SELECT v FROM gate WHERE id = 1").first("v");
    const batched = await env.DB.batch([
      env.DB.prepare("SELECT COUNT(*) AS n FROM gate"),
      env.DB.prepare("SELECT v FROM gate WHERE id = 2"),
    ]);
    const execed = await env.DB.exec("SELECT 3");
    return {
      first: one,
      batchCounts: batched.map((r) => r.results.length),
      execCount: execed.count,
    };
  },
  'd1-vec-query': async (env) => {
    const version = await env.DB.prepare("SELECT vec_version() AS v").first("v");
    await env.DB.exec(
      "CREATE VIRTUAL TABLE IF NOT EXISTS vec_probe USING vec0(embedding float[3]); " +
      "DELETE FROM vec_probe;"
    );
    await env.DB.exec(
      "INSERT INTO vec_probe(rowid, embedding) VALUES (1, '[1, 0, 0]'), (2, '[0, 1, 0]')"
    );
    const top = await env.DB.prepare(
      "SELECT rowid AS id FROM vec_probe WHERE embedding MATCH '[1, 0, 0]' ORDER BY distance LIMIT 1"
    ).first("id");
    return { version, top };
  },
  'd1-fetch-query': async (env) => {
    const res = await stubPost(env.DB._stub, {
      statements: [{ sql: "SELECT v FROM gate WHERE id = 1", params: [] }],
    });
    const body = await res.json();
    return { status: res.status, rows: body.result && body.result[0] && body.result[0].rows };
  },
  'd1-fetch-query-top-marker': async (env) => {
    const res = await stubPost(env.DB._stub, {
      statements: [{ sql: "SELECT v FROM gate WHERE id = 1", params: [] }],
      [MARK]: true,
    });
    const body = await res.json();
    return { status: res.status, rows: body.result && body.result[0] && body.result[0].rows };
  },
  'd1-fetch-query-nested-marker': async (env) => {
    const res = await stubPost(env.DB._stub, {
      statements: [{ sql: "SELECT 'nested'", params: [], [MARK]: true }],
    });
    const body = await res.json();
    return { status: res.status, count: Array.isArray(body.result) ? body.result.length : -1 };
  },
  'd1-rpc-export': async (env) => {
    try {
      const dump = await env.DB._stub.__d1Export({});
      return { threw: false, sqlBytes: (dump.sql || "").length };
    } catch (e) { return { threw: true, name: e.name, message: String(e.message || e) }; }
  },
  'd1-rpc-migrate': async (env) => {
    try {
      await env.DB._stub.__d1Migrate("probe_x.sql", "SELECT 1", "d1_migrations");
      return { threw: false };
    } catch (e) { return { threw: true, name: e.name, message: String(e.message || e) }; }
  },
  'd1-fetch-export': async (env) => {
    const res = await stubPost(env.DB._stub, { export: {} });
    return { status: res.status, body: (await res.text()).slice(0, 300) };
  },
  'd1-fetch-migrate': async (env) => {
    const res = await stubPost(env.DB._stub, {
      migrate: { name: "probe_x.sql", sql: "SELECT 1", table: "d1_migrations" },
    });
    return { status: res.status, body: (await res.text()).slice(0, 300) };
  },
  'd1-fetch-exec-rows': async (env) => {
    const res = await stubPost(env.DB._stub, { exec: { sql: "SELECT 1", rows: true } });
    return { status: res.status, body: (await res.text()).slice(0, 300) };
  },
  'd1-rpc-apply': async (env) => {
    try {
      const result = await env.DB._stub.__d1Query({ sql: "SELECT 'apply'", [MARK]: true });
      return {
        threw: false,
        isDump: result !== null && typeof result === "object" && typeof result.sql === "string",
        keys: result !== null && typeof result === "object" ? Object.keys(result) : [],
      };
    } catch (e) { return { threw: true, name: e.name, message: String(e.message || e) }; }
  },
  'kv-binding-ok': async (env) => {
    await env.NS.put("k1", "v1");
    const got = await env.NS.get("k1");
    await env.NS.delete("k1");
    const gone = await env.NS.get("k1");
    return { got, gone };
  },
  'kv-fetch-get': async (env) => {
    await env.NS.put("k1", "v1");
    const res = await stubPost(env.NS._stub, { op: "get", key: "k1" });
    const body = await res.json();
    return { status: res.status, found: body.result && body.result.found, error: body.error ?? null };
  },
  'kv-fetch-get-marker': async (env) => {
    await env.NS.put("k1", "v1");
    const res = await stubPost(env.NS._stub, { op: "get", key: "k1", [MARK]: true });
    const body = await res.json();
    return { status: res.status, found: body.result && body.result.found, error: body.error ?? null };
  },
  'kv-fetch-put-marker': async (env) => {
    const res = await stubPost(env.NS._stub, {
      op: "put",
      key: "k2",
      value: [...new TextEncoder().encode("v2")],
      [MARK]: true,
    });
    const body = await res.json();
    return { status: res.status, ok: body.result && body.result.ok, error: body.error ?? null };
  },
  'kv-get-k2': async (env) => ({ value: await env.NS.get("k2") }),
  'qsend': async (env) => {
    await env.Q.sendBatch([{ body: { n: 1 } }, { body: { n: 2 } }, { body: { n: 3 } }]);
    return { sent: true };
  },
  'qinfo': async (env) => {
    const res = await stubPost(env.Q._stub, { op: "info" });
    const body = await res.json();
    return { status: res.status, paused: body.result && body.result.paused, error: body.error ?? null };
  },
  'qinfo-marker': async (env) => {
    const res = await stubPost(env.Q._stub, { op: "info", [MARK]: true });
    const body = await res.json();
    return { status: res.status, paused: body.result && body.result.paused, error: body.error ?? null };
  },
  'q-pollution-gate': async (env) => {
    // The host script's top level overrode `Set.prototype.has` so the
    // queue's RPC allowlist answers true for the operator methods and
    // the content-type check answers true for "evil-type". The shared
    // queue cell runs in the runtime pool, whose isolates evaluate no
    // tenant bundle, so the redrive and the forged DLQ batch still
    // refuse as not-a-function and the bad content type still refuses
    // in validation — while a well-formed direct send lands.
    const enc = new TextEncoder();
    const redrive = await rpcProbe(() => env.Q._stub._redrive(10));
    const dlqForge = await rpcProbe(
      () => env.Q._stub.__queueDlqAcceptBatch({
        transfers: [{
          transferId: crypto.randomUUID(),
          deadLetterSource: "evil-source-q",
          entry: {
            body: enc.encode("dlq-evil"),
            contentType: "text",
          },
        }],
      }),
    );
    const sendBadType = await rpcProbe(
      () => env.Q._stub.__queueSend({ entries: [{
        body: enc.encode("hi"),
        contentType: "evil-type",
        size: 2,
        delaySeconds: 0,
      }] }),
    );
    const sendDirectOk = await rpcProbe(
      () => env.Q._stub.__queueSend({ entries: [{
        body: enc.encode("direct-ok"),
        contentType: "text",
        size: 9,
        delaySeconds: 0,
      }] }),
    );
    return { redrive, dlqForge, sendBadType, sendDirectOk };
  },
  'qconsumed': async (env) => {
    const n = await env.DB.prepare("SELECT COUNT(DISTINCT id) AS n FROM consumed").first("n");
    return { count: n };
  },
};

export default {
  async fetch(req, env, ctx) {
    const t = new URL(req.url).pathname.slice(1);
    const run = handlers[t];
    if (run === undefined) return J({ test: t, ok: false, error: `unknown ${t}` }, 404);
    try {
      return J({ test: t, ok: true, result: await run(env) });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
  // Records every delivery in D1; returning acknowledges the batch.
  async queue(batch, env) {
    for (const message of batch.messages) {
      await env.DB.prepare("INSERT INTO consumed (id) VALUES (?)").bind(message.id).run();
    }
  },
};
