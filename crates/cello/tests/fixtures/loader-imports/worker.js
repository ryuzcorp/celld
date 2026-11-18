// Modified by ryuzcorp (ryuzcorp/celld): loader sibling-imports fixture (upstream #304). Apache-2.0.
const MAIN = `import { WorkerEntrypoint } from "cloudflare:workers";
import { probe } from "./lib.js";
export default class extends WorkerEntrypoint { fetch() { return Response.json(probe()); } }`;
const LIB = `import { RpcTarget, waitUntil, env } from "cloudflare:workers";
export const probe = () => ({ RpcTarget: typeof RpcTarget, waitUntil: typeof waitUntil, env: typeof env });`;

export default {
  async fetch(req, env) {
    if (new URL(req.url).pathname === "/health") return new Response("ok");
    const w = env.LOADER.get("x", () => ({
      compatibilityDate: "2026-01-01",
      mainModule: "main.js",
      modules: { "main.js": MAIN, "lib.js": LIB },
    }));
    try {
      return await w.getEntrypoint().fetch(req);
    } catch (e) {
      return new Response(String(e) + "\n", { status: 500 });
    }
  },
};
