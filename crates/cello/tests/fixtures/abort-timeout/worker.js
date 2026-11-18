// Modified by ryuzcorp (ryuzcorp/celld): AbortSignal.timeout admission fixture (upstream #306). Apache-2.0.
import { DurableObject } from "cloudflare:workers";

export class Probe extends DurableObject {
  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/timeout-signal") {
      // Unused deadline, dropped on the floor: must not hold the
      // request's admission slot until it fires. 60 s outlives the test
      // loop's 70 sequential requests, so the loop cannot slip past the
      // timers' life on a slow runner and pass silently.
      AbortSignal.timeout(60000);
      return new Response("ok");
    }
    if (url.pathname === "/fires") {
      // The timeout itself must still abort.
      const signal = AbortSignal.timeout(50);
      const reason = await new Promise((resolve) => {
        signal.addEventListener("abort", () =>
          resolve(signal.reason?.name ?? "aborted"),
        );
      });
      return Response.json({ reason });
    }
    return new Response("ok");
  }
}

export default {
  async fetch(req, env) {
    if (new URL(req.url).pathname === "/health") return new Response("ok");
    return env.PROBE.get(env.PROBE.idFromName("probe")).fetch(req);
  },
};
