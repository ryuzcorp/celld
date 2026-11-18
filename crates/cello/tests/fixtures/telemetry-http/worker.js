// Modified by Ryuz Corp (Cello): incoming method/status fixture (upstream #285). Apache-2.0.
import { DurableObject } from "cloudflare:workers";

export class Probe extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/do-fail") {
      return new Response("do-fail", { status: 500 });
    }
    return new Response(`do-ok ${request.method}`);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/fail") {
      return new Response("probe-response", { status: 503 });
    }
    if (url.pathname === "/do-ok" || url.pathname === "/do-fail") {
      // Re-issue the call with a method of our own: the worker span records
      // GET while the DO's cell fetch span must record the rewritten
      // method, so the export tells the two apart (denoland/celld#285).
      const method = url.pathname === "/do-ok" ? "POST" : "PUT";
      const stub = env.PROBE.get(env.PROBE.idFromName("probe"));
      const upstream = await stub.fetch(
        new Request(`http://do${url.pathname}`, { method }),
      );
      const body = await upstream.text();
      if (url.pathname === "/do-fail") {
        // Mask the 500: the worker answers 200, so the 500 error text in
        // the export can only come from the DO's span.
        return Response.json({ doStatus: upstream.status, body });
      }
      return Response.json({ status: upstream.status, body });
    }
    return new Response("ok");
  },
};
