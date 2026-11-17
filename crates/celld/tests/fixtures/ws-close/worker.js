// Modified by ryuzcorp (ryuzcorp/celld): server-initiated close fixture (upstream #279). Apache-2.0.
export default {
  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/health") return new Response("ok");
    if (url.pathname === "/validate") {
      const code = Number(url.searchParams.get("code"));
      const pair = new WebSocketPair();
      const server = pair[1];
      server.accept();
      try {
        server.close(code, "x");
        return Response.json({ threw: null });
      } catch (e) {
        return Response.json({ threw: e.name + ": " + e.message });
      }
    }
    // A WebSocket whose server end refuses the first message with 1008.
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener("message", () => server.close(1008, "refused"));
    return new Response(null, { status: 101, webSocket: client });
  },
};
