// Modified by ryuzcorp (ryuzcorp/celld): D1 export operator-gate fixture. Apache-2.0.
// A Worker with its own D1 binding probes the operator-only surface: stub RPC
// `__d1Export`/`__d1Migrate` and the `fetch` export/migrate/exec-rows branches
// must refuse, while the binding's own query/batch/exec and the fetch query
// branch keep working. Every probe uses the Worker's own bound database, so
// the suite passes whether or not a forged binding for another database is
// possible.
const J = (o, st = 200) => Response.json(o, { status: st });

const stubPost = (stub, body) =>
  stub.fetch("http://cell/", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

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
