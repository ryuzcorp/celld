// Modified by Ryuz Corp (Cello): private-CA TLS fixture (upstream #230). Apache-2.0.
import { connect } from "cloudflare:sockets";

const readAll = async (readable) => {
  const reader = readable.getReader();
  const chunks = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  reader.releaseLock();
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(out);
};

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");
    if (url.pathname === "/fetch-tls") {
      const port = url.searchParams.get("port");
      try {
        const upstream = await fetch(`https://127.0.0.1:${port}/`);
        return Response.json({
          status: upstream.status,
          body: await upstream.text(),
        });
      } catch (error) {
        return Response.json(
          { error: String((error && error.message) || error) },
          { status: 500 },
        );
      }
    }
    if (url.pathname === "/starttls") {
      const port = url.searchParams.get("port");
      try {
        const socket = connect(`127.0.0.1:${port}`, { secureTransport: "starttls" });
        await socket.opened;
        const tls = socket.startTls({ expectedServerHostname: "localhost" });
        await tls.opened;
        const writer = tls.writable.getWriter();
        await writer.write(
          new TextEncoder().encode("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
        );
        writer.releaseLock();
        const body = await readAll(tls.readable);
        await tls.close();
        return Response.json({ body });
      } catch (error) {
        return Response.json(
          { error: String((error && error.message) || error) },
          { status: 500 },
        );
      }
    }
    return new Response("not found", { status: 404 });
  },
};
