// Modified by ryuzcorp (ryuzcorp/celld): retired-continuation resume regression fixture (upstream #277). Apache-2.0.
export class Repro {
  constructor(state) {
    this.state = state;
    this.results = [];

    this.job = new Promise((resolve) => {
      this.resolveJob = resolve;
    });

    // Start work from the constructor event without keeping that event
    // alive. A later event resolves the gate; the continuation must then
    // still perform outbound I/O and input-gated writes.
    state.blockConcurrencyWhile(async () => {
      void this.runJob();
    });
  }

  async runJob() {
    try {
      await this.job;
      this.results.push('awaited');

      // The test passes the node's own address as ?origin= so the fetch
      // needs no external network; it still exercises a real outbound
      // subrequest from the resumed continuation.
      await fetch(`${this.fetchUrl}/plain`);
      this.results.push('fetched');

      await this.state.storage.put('result', 'ok');
      this.results.push('stored');
    } catch (error) {
      this.results.push(`error: ${error.message}`);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/resolve') {
      this.fetchUrl = url.searchParams.get('origin');
      this.resolveJob();
      return Response.json({ resolved: true });
    }
    if (url.pathname === '/log') {
      return Response.json(this.results);
    }
    return new Response('ok');
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') return new Response('ok');
    if (url.pathname === '/plain') return new Response('plain ok');
    return env.REPRO.get(env.REPRO.idFromName('repro')).fetch(request);
  },
};
