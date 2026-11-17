import { WorkflowEntrypoint } from "cloudflare:workers";

export class Slow extends WorkflowEntrypoint {
  async run(event, step) {
    let done = 0;
    for (let i = 1; i <= 12; i++) {
      done = await step.do(`step ${i}`, async () => {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        return i;
      });
    }
    return { done };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "a";
    if (url.pathname === "/start") {
      await env.SLOW.create({ id });
      return new Response("started\n");
    }
    return Response.json(await (await env.SLOW.get(id)).status());
  },
};
