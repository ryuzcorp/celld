// Modified by ryuzcorp (ryuzcorp/celld): post-response outbound dial fixture (upstream #254). Apache-2.0.
import { DurableObject } from "cloudflare:workers";
export class Dialer extends DurableObject {
  log = [];
  async dial(url) {
    const t0 = Date.now();
    const note = (line) => this.log.push(`+${Date.now() - t0}ms ${line}`);
    try {
      const ws = (await fetch(url, { headers: { Upgrade: "websocket" } })).webSocket;
      ws.accept();
      note("open");
      ws.addEventListener("message", (e) => note(`message ${e.data}`));
      ws.addEventListener("close", (e) => note(`close ${e.code} ${e.reason}`));
      ws.send("1");
      setTimeout(() => ws.send("2"), 1000);
    } catch (e) {
      note(`threw ${e.message}`);
    }
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");
    const echo = `${url.origin}/echo`;
    if (url.pathname === "/now") {
      await this.dial(echo);
      return new Response("dialed\n");
    }
    if (url.pathname === "/later") {
      new Promise((resolve) => setTimeout(resolve, 100)).then(() => this.dial(echo));
      return new Response("will dial\n");
    }
    return Response.json(this.log);
  }
}
export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");
    if (url.pathname === "/echo") {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("message", (e) => server.send(`echo ${e.data}`));
      return new Response(null, { status: 101, webSocket: client });
    }
    return env.DIALER.get(env.DIALER.idFromName(url.searchParams.get("name") ?? "a")).fetch(request);
  },
};
