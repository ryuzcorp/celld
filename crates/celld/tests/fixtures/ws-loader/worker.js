// Modified by ryuzcorp (ryuzcorp/celld): loader 101 upgrade fixture (upstream #305). Apache-2.0.
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

function echo() {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  server.addEventListener("message", (e) => server.send("echo:" + e.data));
  return new Response(null, { status: 101, webSocket: client });
}

export class Room extends DurableObject {
  fetch() {
    return echo();
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

export default {
  async fetch(req, env, ctx) {
    const p = new URL(req.url).pathname;
    if (p === "/health") return new Response("ok");
    if (p === "/direct") return echo();
    if (p === "/do")
      return env.ROOM.get(env.ROOM.idFromName("r")).fetch(req);
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
