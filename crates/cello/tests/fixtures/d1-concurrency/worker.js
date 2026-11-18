// Modified by ryuzcorp (ryuzcorp/celld): D1 concurrent-call fixture; cross-request attribution fixtures. Apache-2.0.
//
// Conformance fixture for concurrent D1 calls from a Worker. Each route
// drives one shape: sequential calls, concurrent calls within one request,
// concurrent requests each doing several calls, and the cached-prepare
// shape parameterized-query drivers use. Every route answers only after
// every D1 call it made has answered.
//
// Every D1 route first awaits the isolate-once shared setup below, the way
// a Worker middleware ensures its schema before serving. Concurrent cold
// requests share one setup pass; each waiter must still answer.
//
// The attribution routes below share a gated setup pass per test: the first
// caller starts it, later callers wait on it, and the test opens the gate
// once every waiter attached. The D1 tail after the gate attributes the
// setup's native work to the starter, so each waiter's continuation resumes
// in the starter's checkpoint — the foreign execution these routes test.
import { existsSync, mkdirSync } from "node:fs";
import { connect } from "cloudflare:sockets";
const SCHEMA = `
CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS u (id TEXT PRIMARY KEY, email TEXT, role TEXT);
`;

let ready = null;
const ensureDb = (db) => {
  if (!ready) {
    ready = (async () => {
      await db.exec(SCHEMA);
      const n = await db.prepare("SELECT count(*) AS n FROM t").first("n");
      if (n === 0) {
        for (let i = 0; i < 5; i++) {
          await db
            .prepare("INSERT INTO t (v) VALUES (?)")
            .bind("v" + i)
            .run();
        }
        await db
          .prepare("INSERT INTO u (id, email, role) VALUES (?, ?, ?)")
          .bind("u1", "a@example.com", "user")
          .run();
      }
      // One more read so the shared pass holds several D1 calls.
      await db.prepare("SELECT email FROM u WHERE id = ?").bind("u1").first();
    })();
  }
  return ready;
};

const attrSetups = {};
const ensureAttrSetup = (db, name) => {
  if (!attrSetups[name]) {
    const setup = { started: false, waiters: 0, gate: null, release: null };
    setup.gate = new Promise((resolve) => {
      setup.release = resolve;
    });
    attrSetups[name] = setup;
    setup.ready = (async () => {
      setup.started = true;
      await setup.gate;
      // Native work attributed to the starter: every waiter resumes in the
      // starter's checkpoint once this answers.
      await db.prepare("SELECT 1 AS one").first("one");
    })();
  }
  return attrSetups[name];
};

// F1: a sync-settling streaming response whose producer suspends on this.
const f1 = { pulled: false, gate: null, release: null };
f1.gate = new Promise((resolve) => {
  f1.release = resolve;
});

// F2: the waiter's early waitUntil writes this row once the second gate
// opens. The write's op is handed to the waiter, so it only runs when the
// waiter's request is still alive — i.e. when its host frame drained into
// the background aggregate instead of leaking behind the scheduled frame.
const f2 = { gate2: null, open2: null };
f2.gate2 = new Promise((resolve) => {
  f2.open2 = resolve;
});

// R1/R2: one second gate per test. The TCP/WS ownership starters hold theirs
// so they provably stay alive while the waiter retires; the rewriter waiter's
// opens only after the harness proved the starter retired.
const r1gates = {};
const r1gate = (name) => {
  if (!r1gates[name]) {
    const gate = {};
    gate.ready = new Promise((resolve) => {
      gate.open = resolve;
    });
    r1gates[name] = gate;
  }
  return r1gates[name];
};

export default {
  async scheduled(_ctrl, _env) {
    // Runs inside the waiter's scheduled() call below: the begin/end event
    // frame wraps this suspension, so the end runs in the starter's
    // checkpoint once the shared setup answers there.
    const setup = attrSetups.f2;
    setup.waiters += 1;
    await setup.ready;
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const db = env.DB;

    if (url.pathname === "/reset") {
      ready = null;
      return Response.json({ ok: true });
    }

    if (url.pathname === "/setup") {
      await ensureDb(db);
      return Response.json({ ok: true });
    }

    if (url.pathname === "/sequential") {
      await ensureDb(db);
      const n = Number.parseInt(url.searchParams.get("n") || "5", 10);
      // Several sequential reads, covering prepare/first/all.
      const firsts = [];
      for (let i = 0; i < n; i++) {
        firsts.push(await db.prepare("SELECT count(*) AS n FROM t").first("n"));
      }
      const { results } = await db.prepare("SELECT v FROM t ORDER BY id").all();
      // One batch and one exec in the same request.
      const batched = await db.batch([
        db.prepare("SELECT 1 AS one").bind(),
        db.prepare("SELECT 2 AS two").bind(),
      ]);
      await db.exec("CREATE TABLE IF NOT EXISTS t2 (id INTEGER PRIMARY KEY)");
      const raw = await db.prepare("SELECT v FROM t ORDER BY id LIMIT 1").raw();
      return Response.json({
        all: results.length,
        batch: batched.length,
        firsts,
        raw,
      });
    }

    if (url.pathname === "/parallel") {
      await ensureDb(db);
      const n = Number.parseInt(url.searchParams.get("n") || "8", 10);
      const out = await Promise.all(
        Array.from({ length: n }, () =>
          db.prepare("SELECT count(*) AS n FROM t").first("n"),
        ),
      );
      return Response.json({ par: out });
    }

    if (url.pathname === "/mixed-parallel") {
      await ensureDb(db);
      // Concurrent calls of different methods in one request.
      const [a, b, c, d, e] = await Promise.all([
        db.prepare("SELECT count(*) AS n FROM t").first("n"),
        db.prepare("SELECT v FROM t").all(),
        db.batch([
          db.prepare("SELECT 1 AS one").bind(),
          db.prepare("SELECT 2 AS two").bind(),
        ]),
        db.prepare("SELECT v FROM t WHERE v = ?").bind("v0").first(),
        db.prepare("SELECT count(*) AS n FROM t").raw(),
      ]);
      return Response.json({
        a,
        b: b.results.length,
        c: c.length,
        d,
        e,
      });
    }

    if (url.pathname === "/cached-prepare") {
      await ensureDb(db);
      // The shape parameterized-query drivers use: one cached prepare,
      // then concurrent bind/all from that same base statement.
      const n = Number.parseInt(url.searchParams.get("n") || "8", 10);
      const base = db.prepare("SELECT v FROM t WHERE v = ?");
      const out = await Promise.all(
        Array.from({ length: n }, (_, i) =>
          base
            .bind("v" + (i % 5))
            .all()
            .then((r) => r.results.length),
        ),
      );
      return Response.json({ cached: out });
    }

    if (url.pathname === "/multi") {
      await ensureDb(db);
      // Several D1 calls per request, for the N-concurrent-requests shape:
      // sequential reads, one join-style read, and one batch.
      const s1 = await db.prepare("SELECT * FROM u WHERE id = ?").bind("u1").first();
      const s2 = await db
        .prepare("SELECT count(*) AS n FROM t")
        .first("n");
      const { results } = await db
        .prepare("SELECT v FROM t ORDER BY id")
        .all();
      const batched = await db.batch([
        db.prepare("SELECT 1 AS one").bind(),
        db.prepare("SELECT email FROM u WHERE id = ?").bind("u1"),
      ]);
      return Response.json({
        all: results.length,
        batch: batched.length,
        s1: s1 !== null,
        s2,
      });
    }

    if (url.pathname === "/one") {
      await ensureDb(db);
      const n = await db.prepare("SELECT count(*) AS n FROM t").first("n");
      return Response.json({ n });
    }

    if (url.pathname === "/attr-status") {
      const setup = attrSetups[url.searchParams.get("name")];
      return Response.json({
        started: setup?.started ?? false,
        waiters: setup?.waiters ?? 0,
      });
    }

    if (url.pathname === "/attr-release") {
      attrSetups[url.searchParams.get("name")].release();
      return Response.json({ ok: true });
    }

    if (url.pathname === "/f1-stream") {
      // Settles in its first turn: the streaming-response bridge pump starts
      // while settling, and the producer suspends on the module gate until
      // the test's release request answers in another entry's turn.
      return new Response(
        new ReadableStream({
          async pull(controller) {
            f1.pulled = true;
            const chunk = await f1.gate;
            controller.enqueue(chunk);
            controller.close();
          },
        }),
        { headers: { "content-type": "text/plain" } },
      );
    }

    if (url.pathname === "/f1-release") {
      f1.release(new TextEncoder().encode("f1-chunk"));
      return Response.json({ ok: true });
    }

    if (url.pathname === "/f1-status") {
      return Response.json({ pulled: f1.pulled });
    }

    if (url.pathname === "/f2-start") {
      const setup = ensureAttrSetup(db, "f2");
      await setup.ready;
      return Response.json({ ok: true });
    }

    if (url.pathname === "/f2-wait") {
      await ensureDb(db);
      // Into the host frame: lost when the scheduled frame below leaks, in
      // which case the request end drains the leaked frame instead and the
      // request retires without waiting for this write.
      ctx.waitUntil(
        f2.gate2.then(() =>
          db.prepare("INSERT INTO t (v) VALUES (?)").bind("f2-row").run(),
        ),
      );
      return Response.json(await env.SELF.scheduled());
    }

    if (url.pathname === "/f2-open2") {
      f2.open2();
      return Response.json({ ok: true });
    }

    if (url.pathname === "/f2-flag") {
      await ensureDb(db);
      const n = await db
        .prepare("SELECT count(*) AS n FROM t WHERE v = ?")
        .bind("f2-row")
        .first("n");
      return Response.json({ present: n > 0 });
    }

    if (url.pathname === "/f3-start") {
      const setup = ensureAttrSetup(db, "f3");
      await setup.ready;
      return Response.json({ ok: true });
    }

    if (url.pathname === "/f3-wait") {
      const marker = "/tmp/f3-waiter";
      mkdirSync(marker, { recursive: true });
      const setup = ensureAttrSetup(db, "f3");
      setup.waiters += 1;
      await setup.ready;
      return Response.json({ seen: existsSync(marker) });
    }

    if (url.pathname === "/f5-start") {
      const setup = ensureAttrSetup(db, "f5");
      await setup.ready;
      return Response.json({ ok: true });
    }

    if (url.pathname === "/f5-forward") {
      const setup = ensureAttrSetup(db, "f5");
      setup.waiters += 1;
      await setup.ready;
      const host = new URL(request.url).host;
      try {
        const echoed = await fetch(`http://${host}/echo-size`, {
          method: "POST",
          body: request.body,
        });
        const { size } = await echoed.json();
        return Response.json({ ok: true, size });
      } catch (error) {
        return Response.json({
          ok: false,
          error: String(error?.message ?? error),
        });
      }
    }

    if (url.pathname === "/echo-size") {
      const bytes = await request.arrayBuffer();
      return Response.json({ size: bytes.byteLength });
    }

    if (url.pathname === "/r1-start") {
      // Starter for the TCP/WS ownership tests: shares the gated setup, then
      // holds the second gate so it provably stays alive while the waiter
      // opens its socket and retires.
      const name = url.searchParams.get("name");
      const setup = ensureAttrSetup(db, name);
      await setup.ready;
      await r1gate(name).ready;
      return Response.json({ ok: true });
    }

    if (url.pathname === "/r1-release2") {
      r1gate(url.searchParams.get("name")).open();
      return Response.json({ ok: true });
    }

    if (url.pathname === "/r1-tcp-wait") {
      const name = url.searchParams.get("name");
      const port = url.searchParams.get("port");
      const setup = ensureAttrSetup(db, name);
      setup.waiters += 1;
      await setup.ready;
      // Foreign checkpoint: this socket must be owned by this request, so
      // this request's retirement closes it while the starter still lives.
      const socket = connect(`127.0.0.1:${port}`);
      const info = await socket.opened;
      return Response.json({ ok: true, remote: info.remoteAddress ?? null });
    }

    if (url.pathname === "/r1-ws-wait") {
      const name = url.searchParams.get("name");
      const port = url.searchParams.get("port");
      const setup = ensureAttrSetup(db, name);
      setup.waiters += 1;
      await setup.ready;
      // Foreign checkpoint: this socket must be owned by this request, so
      // this request's retirement closes it while the starter still lives.
      const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
      await new Promise((resolve, reject) => {
        ws.addEventListener("open", resolve, { once: true });
        ws.addEventListener(
          "error",
          (event) => reject(new Error(event.message)),
          { once: true },
        );
      });
      return Response.json({ ok: true });
    }

    if (url.pathname === "/r1hr-start") {
      // Starter for the rewriter test: connects first, so the harness
      // observing EOF on that connection proves this request retired — and,
      // before the fix, that it freed the waiter's mis-tracked rewriter.
      const port = url.searchParams.get("port");
      const socket = connect(`127.0.0.1:${port}`);
      await socket.opened;
      const setup = ensureAttrSetup(db, "r1hr");
      await setup.ready;
      return Response.json({ ok: true });
    }

    if (url.pathname === "/r1hr-wait") {
      const setup = ensureAttrSetup(db, "r1hr");
      setup.waiters += 1;
      await setup.ready;
      // Foreign checkpoint: this rewriter must be owned by this request, so
      // the starter's retirement cannot free its parser thread.
      const rewritten = new HTMLRewriter()
        .on("p", {
          element(el) {
            el.setInnerContent("bye");
          },
        })
        .transform(
          new Response("<p>hello</p>", {
            headers: { "content-type": "text/html" },
          }),
        );
      // The second gate opens only after the harness proved the starter
      // retired, so this read provably runs after the wrong owner's cleanup.
      await r1gate("r1hr").ready;
      try {
        const text = await rewritten.text();
        return Response.json({ ok: true, text });
      } catch (error) {
        return Response.json({
          ok: false,
          error: String(error?.message ?? error),
        });
      }
    }

    if (url.pathname === "/r2-tee-start") {
      const setup = ensureAttrSetup(db, "r2tee");
      await setup.ready;
      return Response.json({ ok: true });
    }

    if (url.pathname === "/r2-tee-forward") {
      const setup = ensureAttrSetup(db, "r2tee");
      setup.waiters += 1;
      await setup.ready;
      // Foreign checkpoint: the tee must replace the stream in this
      // request's set, so the forwarded branch transfers as owned.
      const host = new URL(request.url).host;
      try {
        const [first, second] = request.body.tee();
        const echoed = await fetch(`http://${host}/echo-size`, {
          method: "POST",
          body: first,
        });
        const { size } = await echoed.json();
        const rest = await new Response(second).arrayBuffer();
        return Response.json({
          ok: true,
          forwarded: size,
          local: rest.byteLength,
        });
      } catch (error) {
        return Response.json({
          ok: false,
          error: String(error?.message ?? error),
        });
      }
    }

    return Response.json({ ok: true });
  },
};
