// s3bench workload app: one SQLite-backed Durable Object per agent id,
// plus a single hot cell shared by every hot writer. Each write bumps a
// counter and records one small per-turn row, the way an agent accumulates
// memory.
export class Agent {
  constructor(state) {
    this.state = state;
  }
  async fetch(request) {
    const url = new URL(request.url);
    // The hot cell serves concurrent writers; storage awaits overlap unless
    // the input gate is held, so two writes could read the same counter.
    // The block serializes the read-modify-write per cell.
    const n = await this.state.blockConcurrencyWhile(async () => {
      const n = (await this.state.storage.get("n")) ?? 0;
      await this.state.storage.put("n", n + 1);
      await this.state.storage.put(`turn:${n}`, {
        seq: n,
        at: new Date().toISOString(),
        url: request.url,
        event: crypto.randomUUID(),
      });
      return n;
    });
    return Response.json({ n: n + 1 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const agent = url.pathname.match(/^\/agent\/([^/]+)\/write$/);
    if (agent) {
      const id = env.AGENT.idFromName(agent[1]);
      return env.AGENT.get(id).fetch(request);
    }
    if (url.pathname === "/hot/write") {
      const id = env.AGENT.idFromName("hot");
      return env.AGENT.get(id).fetch(request);
    }
    return new Response("not found", { status: 404 });
  },
};
