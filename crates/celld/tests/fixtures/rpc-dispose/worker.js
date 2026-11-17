// Modified by ryuzcorp (ryuzcorp/celld): RPC disposal fixture (upstream #260). Apache-2.0.
import { WorkerEntrypoint } from "cloudflare:workers";

export class ExampleService extends WorkerEntrypoint {
  record() {
    return { value: "example" };
  }
  array() {
    return ["example"];
  }
  primitive() {
    return 1;
  }
}

export default {
  async fetch(request, _env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");
    const kind = url.searchParams.get("kind") ?? "record";
    if (!["record", "array", "primitive"].includes(kind))
      return new Response("Invalid kind", { status: 400 });

    const result = await ctx.exports.ExampleService[kind]();
    const hasDispose = typeof result[Symbol.dispose] === "function";

    if (url.pathname === "/inspect") return Response.json({ result, hasDispose });

    try {
      using disposable = result;
      return Response.json({ result: disposable, hasDispose, using: "ok" });
    } catch (error) {
      return Response.json({ hasDispose, error: String(error) }, { status: 500 });
    }
  },
};
