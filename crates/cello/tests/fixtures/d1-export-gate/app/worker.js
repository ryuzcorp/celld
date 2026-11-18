// Modified by ryuzcorp (ryuzcorp/celld): D1 export operator-gate fixture; Queue/KV operator fetch gate; Queue/KV stub-RPC allowlists; Queue DLQ RPC refusal; Queue send cell-side validation; KV prototype-safe operator lookup. Apache-2.0.
// A Worker with its own D1 binding probes the operator-only surface: stub RPC
// `__d1Export`/`__d1Migrate` and the `fetch` export/migrate/exec-rows branches
// must refuse, while the binding's own query/batch/exec and the fetch query
// branch keep working. Every probe uses the Worker's own bound database, so
// the suite passes whether or not a forged binding for another database is
// possible. The same shape covers the KV and Queue operator-only fetch
// branches below: stub.fetch() without the host-set marker is refused, the
// bindings keep working, and host-driven queue settlement stays open. Stub
// RPC on the Queue and KV scopes answers the same per-class allowlist shape
// as D1: operator and internal cell methods refuse as "is not a function".
const J = (o, st = 200) => Response.json(o, { status: st });

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

export default {
  async fetch(req, env, ctx) {
    const t = new URL(req.url).pathname.slice(1);
    const run = async () => {
      switch (t) {
        case 'health': return { ok: true };
        case 'seed': {
          await env.DB.exec(
            "CREATE TABLE IF NOT EXISTS gate (id INTEGER PRIMARY KEY, v TEXT); " +
            "DELETE FROM gate; " +
            "INSERT INTO gate VALUES (1, 'one'), (2, 'two');"
          );
          return { seeded: true };
        }
        case 'rpc-export': {
          try {
            const dump = await env.DB._stub.__d1Export({});
            return { threw: false, bytes: (dump.sql || "").length };
          } catch (e) { return { threw: true, name: e.name, message: String(e.message || e) }; }
        }
        case 'fetch-export': {
          const res = await stubPost(env.DB._stub, { export: {} });
          return { status: res.status, body: (await res.text()).slice(0, 300) };
        }
        case 'fetch-forged-operator': {
          // A tenant that passes an operator init key gains nothing: the
          // Request constructor honors no such key, and the dispatch builds
          // a fresh server-side Request that carries no membership.
          const res = await env.DB._stub.fetch(new Request("http://cell/", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ export: {} }),
            __celldOperatorIngress: true,
          }));
          return { status: res.status, body: (await res.text()).slice(0, 300) };
        }
        case 'rpc-migrate': {
          try {
            await env.DB._stub.__d1Migrate("0001_x.sql", "SELECT 1", "d1_migrations");
            return { threw: false };
          } catch (e) { return { threw: true, name: e.name, message: String(e.message || e) }; }
        }
        case 'fetch-migrate': {
          const res = await stubPost(env.DB._stub, {
            migrate: { name: "0001_x.sql", sql: "SELECT 1", table: "d1_migrations" },
          });
          return { status: res.status, body: (await res.text()).slice(0, 300) };
        }
        case 'fetch-exec-rows': {
          const res = await stubPost(env.DB._stub, {
            exec: { sql: "SELECT 1", rows: true },
          });
          return { status: res.status, body: (await res.text()).slice(0, 300) };
        }
        case 'binding-ok': {
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
        }
        case 'fetch-query': {
          const res = await stubPost(env.DB._stub, {
            statements: [{ sql: "SELECT v FROM gate WHERE id = 1", params: [] }],
          });
          const body = await res.json();
          return { status: res.status, rows: body.result && body.result[0] && body.result[0].rows };
        }
        case 'kv-gate': {
          const put = await stubPost(env.KV._stub, {
            op: "put", key: "probe", value: [1, 2, 3],
          });
          const get = await stubPost(env.KV._stub, { op: "get", key: "probe" });
          const info = await stubPost(env.KV._stub, { op: "info" });
          const del = await stubPost(env.KV._stub, { op: "delete", keys: ["probe"] });
          const list = await stubPost(env.KV._stub, { op: "list", prefix: "", limit: 10 });
          const put64 = await stubPost(env.KV._stub, {
            op: "put-base64", key: "probe64", value: "aGk=",
          });
          return {
            put: { status: put.status, body: (await put.text()).slice(0, 300) },
            get: { status: get.status, body: (await get.text()).slice(0, 300) },
            info: { status: info.status, body: (await info.text()).slice(0, 300) },
            delete: { status: del.status, body: (await del.text()).slice(0, 300) },
            list: { status: list.status, body: (await list.text()).slice(0, 300) },
            "put-base64": { status: put64.status, body: (await put64.text()).slice(0, 300) },
          };
        }
        case 'kv-gate-proto': {
          // Inherited names are tenant input to the operator lookup: they
          // must fall through to "unknown operation", never resolve (and
          // leak) a native function into a 403. A JSON null body parses
          // without throwing, so it needs its own 400.
          const shape = async (res) => ({
            status: res.status,
            body: (await res.text()).slice(0, 300),
          });
          const constructorOp = await shape(await stubPost(env.KV._stub, {
            op: "constructor",
          }));
          const toStringOp = await shape(await stubPost(env.KV._stub, {
            op: "toString",
          }));
          const protoOp = await shape(await stubPost(env.KV._stub, {
            op: "__proto__",
          }));
          const nullBody = await shape(await stubPost(env.KV._stub, null));
          return { constructorOp, toStringOp, protoOp, nullBody };
        }
        case 'queue-gate-null': {
          // The Queue fetch compares `body.op` literally, so inherited
          // names cannot resolve — but a JSON null body still throws
          // inside the handler's try and must answer 400, not 500.
          const res = await stubPost(env.Q._stub, null);
          return { status: res.status, body: (await res.text()).slice(0, 300) };
        }
        case 'kv-rpc-gate': {
          // Internal cell methods a binding never calls: the sweeper runs as
          // a direct call inside the cell's own alarm, and metrics as a
          // direct call inside the marker-gated fetch `info` branch.
          const sweep = await rpcProbe(
            () => env.KV._stub.__kvSweep({ now: Date.now(), limit: 10 }),
          );
          const metrics = await rpcProbe(
            () => env.KV._stub.__kvMetrics({ now: Date.now() }),
          );
          return { sweep, metrics };
        }
        case 'kv-binding-ok': {
          await env.KV.put("gate-key", "gate-value");
          const value = await env.KV.get("gate-key");
          const withMeta = await env.KV.getWithMetadata("gate-key");
          await env.KV.put("gate-doomed", "x");
          await env.KV.delete("gate-doomed");
          const gone = await env.KV.get("gate-doomed");
          const listed = await env.KV.list({ prefix: "gate-" });
          return {
            value,
            metaValue: withMeta.value,
            gone,
            names: listed.keys.map((k) => k.name).sort(),
            complete: listed.list_complete,
          };
        }
        case 'queue-gate': {
          const info = await stubPost(env.Q._stub, { op: "info" });
          const purge = await stubPost(env.Q._stub, { op: "purge" });
          const pause = await stubPost(env.Q._stub, { op: "pause" });
          const resume = await stubPost(env.Q._stub, { op: "resume" });
          const peek = await stubPost(env.Q._stub, { op: "peek", limit: 5 });
          const redrive = await stubPost(env.Q._stub, { op: "redrive", limit: 5 });
          return {
            info: { status: info.status, body: (await info.text()).slice(0, 300) },
            purge: { status: purge.status, body: (await purge.text()).slice(0, 300) },
            pause: { status: pause.status, body: (await pause.text()).slice(0, 300) },
            resume: { status: resume.status, body: (await resume.text()).slice(0, 300) },
            peek: { status: peek.status, body: (await peek.text()).slice(0, 300) },
            redrive: { status: redrive.status, body: (await redrive.text()).slice(0, 300) },
          };
        }
        case 'queue-rpc-gate': {
          // Operator cell methods that enter over marker-gated `fetch`, never
          // RPC. The DLQ accept refuses the same way: the cell's own
          // `_driveDlq` forward rides the `__queue_dlq_forward` host op,
          // never stub RPC, so even a well-shaped batch is not a function
          // here — and neither is the reviewer's forged batch below.
          const purge = await rpcProbe(() => env.Q._stub._purge());
          const peek = await rpcProbe(() => env.Q._stub._peek(5));
          const setPaused = await rpcProbe(() => env.Q._stub._setPaused(true));
          const redrive = await rpcProbe(() => env.Q._stub._redrive(5));
          const dlqAccept = await rpcProbe(
            () => env.Q._stub.__queueDlqAcceptBatch({ transfers: [] }),
          );
          const dlqForge = await rpcProbe(
            () => env.Q._stub.__queueDlqAcceptBatch({
              transfers: [{
                transferId: crypto.randomUUID(),
                deadLetterSource: "evil-source-q",
                entry: {
                  id: "forge-" + crypto.randomUUID(),
                  body: new TextEncoder().encode("dlq-evil"),
                  contentType: "text",
                  size: 8,
                },
              }],
            }),
          );
          // Direct `__queueSend` bypasses the binding's encoder, so the cell
          // validates every entry itself: a lying size, an unknown content
          // type, an oversize body, and a bad delay all refuse. A
          // well-formed direct send still lands.
          const enc = new TextEncoder();
          const sendSizeLie = await rpcProbe(
            () => env.Q._stub.__queueSend({ entries: [{
              body: enc.encode("hi"),
              contentType: "text",
              size: 999999999,
              delaySeconds: 0,
            }] }),
          );
          const sendBadType = await rpcProbe(
            () => env.Q._stub.__queueSend({ entries: [{
              body: enc.encode("hi"),
              contentType: "evil-type",
              size: 2,
              delaySeconds: 0,
            }] }),
          );
          const sendOversize = await rpcProbe(() => {
            const body = enc.encode("x".repeat(200 * 1000));
            return env.Q._stub.__queueSend({ entries: [{
              body,
              contentType: "text",
              size: body.byteLength,
              delaySeconds: 0,
            }] });
          });
          const sendBadDelay = await rpcProbe(
            () => env.Q._stub.__queueSend({ entries: [{
              body: enc.encode("hi"),
              contentType: "text",
              size: 2,
              delaySeconds: -1,
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
          return {
            purge, peek, setPaused, redrive, dlqAccept, dlqForge,
            sendSizeLie, sendBadType, sendOversize, sendBadDelay,
            sendDirectOk,
          };
        }
        case 'queue-settle-open': {
          // Host-driven settlement stays open: a body with no lease 409s
          // instead of 403ing.
          const res = await env.Q._stub.fetch("http://cell/__qSettle", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({}),
          });
          return { status: res.status, body: (await res.text()).slice(0, 200) };
        }
        case 'queue-binding-ok': {
          await env.Q.send({ hello: "queue" });
          await env.Q.sendBatch([{ body: { n: 1 } }, { body: { n: 2 } }]);
          const metrics = await env.Q.metrics();
          // No consumer is configured, so all three sends stay backlogged.
          return { sent: true, backlog: metrics.backlogCount };
        }
        default: return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
