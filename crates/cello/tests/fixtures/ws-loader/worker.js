// Modified by Ryuz Corp (Cello): loader 101 upgrade fixture (upstream #305). Apache-2.0.
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

function echo() {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  server.addEventListener("message", (e) => server.send("echo:" + e.data));
  return new Response(null, { status: 101, webSocket: client });
}

// Durable Object sockets record what they receive, one slow frame at a time:
// a synchronous busy-loop per message holds the pump back deterministically,
// so a flood queued behind it is still staged when the close arrives. The
// recording lets a test assert delivery order after the fact. The loop
// accumulates into a global so it cannot be optimized away, and it avoids
// the wall clock, which does not advance mid-turn in the sandbox.
const ROOM_MESSAGES = [];
const ROOM_CLOSES = [];
let ROOM_SPIN = 0;
function roomSlow() {
  let x = 0;
  for (let i = 0; i < 200000000; i++) x += i;
  ROOM_SPIN = (ROOM_SPIN + x) % 1000000007;
}
export class Room extends DurableObject {
  fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/messages") {
      return Response.json({ messages: ROOM_MESSAGES, closes: ROOM_CLOSES });
    }
    const closeAfterEcho = url.searchParams.get("close-after-echo") === "1";
    const closeSync = url.searchParams.get("close-sync") === "1";
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener("message", (e) => {
      if (closeSync) {
        // Echo and close synchronously: no timers, no slow-reader burn.
        // Both frames reach the pipe before the caller can ask for its
        // next event, so the test below observes post-teardown behavior
        // deterministically.
        ROOM_MESSAGES.push(e.data);
        server.send("echo:" + e.data);
        try { server.close(1000, "bye"); } catch {}
        return;
      }
      roomSlow();
      ROOM_MESSAGES.push(e.data);
      server.send("echo:" + e.data);
      if (closeAfterEcho) setTimeout(() => { try { server.close(1000, "bye"); } catch {} }, 50);
    });
    server.addEventListener("close", (e) => {
      ROOM_CLOSES.push({ code: e.code, reason: e.reason, wasClean: e.wasClean });
    });
    return new Response(null, { status: 101, webSocket: client });
  }
}

export class RoomGate extends WorkerEntrypoint {
  fetch(req) {
    return this.env.ROOM.get(this.env.ROOM.idFromName("r")).fetch(req);
  }
}

// The loaded worker makes the socket itself.
const SELF = `export default {
  fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener("message", (e) => server.send("echo:" + e.data));
    return new Response(null, { status: 101, webSocket: client });
  }
};`;
// The loaded worker forwards the upgrade to a Durable Object.
const VIA = `export default {
  fetch(req, env) { return env.ROOM_GATE.fetch(req); }
};`;
// The loaded worker accepts but stops reading on the first message: the
// throwing listener propagates out of the pump loop, so the isolate never
// drains its input queue again and caller frames pile up behind backpressure
// until the queue is full.
const NEVER_READ = `export default {
  fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener("message", () => { throw new Error("stop reading"); });
    return new Response(null, { status: 101, webSocket: client });
  }
};`;

export default {
  async fetch(req, env, ctx) {
    const p = new URL(req.url).pathname;
    if (p === "/health") return new Response("ok");
    if (p === "/direct") return echo();
    if (p === "/do")
      return env.ROOM.get(env.ROOM.idFromName("r")).fetch(req);
    if (p === "/caller-close") {
      // The caller accepts the loaded socket itself and closes it: the
      // close it sent must come back as its own close event, not as an
      // abnormal shutdown.
      const w = env.LOADER.get("closer", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": SELF },
        env: {},
      }));
      const res = await w.getEntrypoint().fetch(new Request("http://x/"));
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      ws.accept();
      ws.close(1000, "done");
      return Response.json(await closed);
    }
    if (p === "/flood-close") {
      // The caller floods a socket the loaded worker never reads, then
      // closes: the close queued behind the flooded frames must still end
      // the pipe and arrive as the caller's own close event instead of
      // hanging the socket.
      const w = env.LOADER.get("never-read", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": NEVER_READ },
        env: {},
      }));
      const res = await w.getEntrypoint().fetch(new Request("http://x/"));
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 20000));
      ws.accept();
      const chunk = "x".repeat(64 * 1024);
      for (let i = 0; i < 48; i++) ws.send(chunk);
      ws.close(1000, "done");
      const result = await Promise.race([closed, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "caller close never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/flood-overload") {
      // Flood past the pipe's staged-bytes cap into a worker that stopped
      // reading and never close: the pipe must tear itself down with 1011
      // instead of staging the flood without bound.
      const w = env.LOADER.get("never-read", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": NEVER_READ },
        env: {},
      }));
      const res = await w.getEntrypoint().fetch(new Request("http://x/"));
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 20000));
      ws.accept();
      const chunk = "x".repeat(64 * 1024);
      for (let i = 0; i < 300; i++) ws.send(chunk);
      const result = await Promise.race([closed, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "overload close never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/big-echo") {
      // One 20 MiB message each way through the isolate-to-isolate pipe: the
      // otherwise-empty admission budget and staging queue each admit one
      // message up to the message cap, so it echoes intact instead of ending
      // loudly with 1011.
      const w = env.LOADER.get("big-echo", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": SELF },
        env: {},
      }));
      const res = await w.getEntrypoint().fetch(new Request("http://x/"));
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const settled = new Promise((resolve) => {
        ws.addEventListener("message", (e) => resolve({ length: e.data.length }));
        ws.addEventListener("close", (e) => resolve({ code: e.code, wasClean: e.wasClean }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 120000));
      ws.accept();
      ws.send("y".repeat(20 * 1024 * 1024));
      const result = await Promise.race([settled, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "big echo never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/order-close") {
      // The caller floods a slow reader, then sends one text and closes: the
      // close queued behind the flood must still deliver every frame first,
      // in order, and then arrive as the caller's own close event.
      const w = env.LOADER.get("order", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": VIA },
        env: { ROOM_GATE: ctx.exports.RoomGate() },
      }));
      const res = await w.getEntrypoint().fetch(
        new Request("http://x/", { headers: { Upgrade: "websocket" } }),
      );
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 20000));
      ws.accept();
      // 8 MiB over a 1 MiB pull queue: only a handful of frames can send
      // before the close is staged, so the close always lands behind a
      // backlog no matter how fast script emission or the drain runs.
      const chunk = "x".repeat(256 * 1024);
      for (let i = 0; i < 33; i++) ws.send(chunk);
      ws.send("last");
      ws.close(1000, "done");
      const result = await Promise.race([closed, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "caller close never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/room-messages") {
      const res = await env.ROOM.get(env.ROOM.idFromName("r")).fetch(
        new Request("http://x/messages"),
      );
      return Response.json(await res.json());
    }
    if (p === "/queued-close") {
      // The caller receives a text, then blocks its event loop before the
      // loaded side closes: the close queued behind the text must still
      // arrive as its own close event instead of a synthetic 1006.
      const w = env.LOADER.get("queued", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": VIA },
        env: { ROOM_GATE: ctx.exports.RoomGate() },
      }));
      const res = await w.getEntrypoint().fetch(
        new Request("http://x/?close-after-echo=1", { headers: { Upgrade: "websocket" } }),
      );
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 20000));
      ws.accept();
      ws.send("hi");
      ws.addEventListener("message", () => {
        // Iteration-counted, accumulated into a global so it cannot be
        // optimized away; must outlast host teardown (milliseconds) so the
        // next `__ws_next` runs after it.
        let x = 0;
        for (let i = 0; i < 60000000; i++) x += i;
        globalThis.__burn = (globalThis.__burn || 0) + x;
      });
      const result = await Promise.race([closed, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "caller close never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/queued-close-sync") {
      // The loaded side echoes and closes synchronously: by the time the
      // caller finishes its post-message burn, the pipe has necessarily
      // torn down, so the next `__ws_next` runs after teardown. A teardown
      // that unregisters the pull endpoint reports a synthetic 1006 here;
      // keeping the endpoint until its terminal is consumed delivers the
      // queued close. No timers on either side: the burn only needs to
      // outlast native pipe teardown, which it does by orders of magnitude.
      const w = env.LOADER.get("queued-sync", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": VIA },
        env: { ROOM_GATE: ctx.exports.RoomGate() },
      }));
      const res = await w.getEntrypoint().fetch(
        new Request("http://x/?close-sync=1", { headers: { Upgrade: "websocket" } }),
      );
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 20000));
      ws.accept();
      ws.send("hi");
      ws.addEventListener("message", () => {
        // Generous iteration-counted burn: the synchronous echo and close
        // are already staged when the text arrives, so teardown necessarily
        // completes (or, post-fix, the endpoint necessarily survives) long
        // before the next `__ws_next` runs.
        let x = 0;
        for (let i = 0; i < 200000000; i++) x += i;
        globalThis.__burn = (globalThis.__burn || 0) + x;
      });
      const result = await Promise.race([closed, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "caller close never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/loaded-queued-close") {
      // The caller floods a slow reader and closes at once: the loaded
      // socket's close event must carry the caller's code even though text
      // was queued ahead of the terminal when the pipe tore down.
      const w = env.LOADER.get("order", () => ({
        compatibilityDate: "2026-01-01",
        mainModule: "m.js",
        modules: { "m.js": VIA },
        env: { ROOM_GATE: ctx.exports.RoomGate() },
      }));
      const res = await w.getEntrypoint().fetch(
        new Request("http://x/", { headers: { Upgrade: "websocket" } }),
      );
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 20000));
      ws.accept();
      ws.send("a");
      ws.send("b");
      ws.close(1000, "done");
      const result = await Promise.race([closed, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "caller close never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/do-queued-close") {
      // Same queued-close shape through a cell target: the caller accepts
      // the Durable Object's socket, receives a text, blocks, and the cell
      // side closes. Its own close event must carry the cell's code.
      const res = await env.ROOM.get(env.ROOM.idFromName("r")).fetch(
        new Request("http://x/?close-after-echo=1", { headers: { Upgrade: "websocket" } }),
      );
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      const timedOut = new Promise((resolve) =>
        setTimeout(() => resolve({ timedOut: true }), 20000));
      ws.accept();
      ws.send("hi");
      ws.addEventListener("message", () => {
        // Iteration-counted, accumulated into a global so it cannot be
        // optimized away; must outlast host teardown (milliseconds) so the
        // next `__ws_next` runs after it.
        let x = 0;
        for (let i = 0; i < 60000000; i++) x += i;
        globalThis.__burn = (globalThis.__burn || 0) + x;
      });
      const result = await Promise.race([closed, timedOut]);
      if (result.timedOut)
        return Response.json({ error: "caller close never arrived" }, { status: 500 });
      return Response.json(result);
    }
    if (p === "/do-caller-close") {
      // Same shape through a cell target: the caller accepts the Durable
      // Object's socket itself and closes it, and its own close event must
      // carry the code it sent rather than an abnormal shutdown.
      const res = await env.ROOM.get(env.ROOM.idFromName("r")).fetch(
        new Request("http://x/", { headers: { Upgrade: "websocket" } }),
      );
      const ws = res.webSocket;
      if (!ws) return Response.json({ error: "no webSocket" }, { status: 500 });
      const closed = new Promise((resolve) => {
        ws.addEventListener("close", (e) => resolve({
          code: e.code, reason: e.reason, wasClean: e.wasClean,
        }));
      });
      ws.accept();
      ws.close(1000, "done");
      return Response.json(await closed);
    }
    const code = p === "/loaded-do" ? VIA : SELF;
    const w = env.LOADER.get(p, () => ({
      compatibilityDate: "2026-01-01",
      mainModule: "m.js",
      modules: { "m.js": code },
      env: { ROOM_GATE: ctx.exports.RoomGate() },
    }));
    return w.getEntrypoint().fetch(req);
  },
};
