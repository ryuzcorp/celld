// Modified by ryuzcorp (ryuzcorp/celld): Queue DLQ forward/redrive fixture. Apache-2.0.
// A Worker with two Queue producers and a failing consumer: `dlq-src`
// dead-letters to `dlq-dst` after its retries are spent. The queue
// handler logs every delivery (queue, id, attempts, body) for the
// fetch routes below, and its mode decides the outcome: `fail` throws,
// `ack` acks, `hold` parks the batch until `/release` acks it. The
// test reads the dead-lettered rows' source and size from the cells'
// SQLite files, which the worker binding never exposes.
const J = (o, st = 200) => Response.json(o, { status: st });

const consumed = [];
let mode = "fail";
let heldResolve = null;

export default {
  async queue(batch) {
    for (const message of batch.messages) {
      consumed.push({
        queue: batch.queue,
        id: message.id,
        attempts: message.attempts,
        body: message.body,
      });
    }
    if (mode === "ack") batch.ackAll();
    else if (mode === "hold") {
      await new Promise((resolve) => {
        heldResolve = resolve;
      });
      batch.ackAll();
    } else {
      throw new Error("dlq-src fails while mode=fail");
    }
  },

  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const t = url.pathname.slice(1);
    const run = async () => {
      switch (t) {
        case 'health':
          return { ok: true };
        case 'send': {
          const which = url.searchParams.get("queue") ?? "src";
          const n = Number(url.searchParams.get("n") ?? "3");
          const prefix = url.searchParams.get("prefix") ?? "dlq-payload";
          const q = which === "dlq" ? env.Q_DLQ : env.Q_SRC;
          const bodies = [];
          for (let i = 0; i < n; i++) {
            const body = `${prefix}-${i}`;
            bodies.push(body);
            await q.send(body, { contentType: "text" });
          }
          return { sent: n, bodies };
        }
        case 'metrics': {
          const src = await env.Q_SRC.metrics();
          const dlq = await env.Q_DLQ.metrics();
          return {
            src: { backlog: src.backlogCount, bytes: src.backlogBytes },
            dlq: { backlog: dlq.backlogCount, bytes: dlq.backlogBytes },
          };
        }
        case 'consumed':
          return { deliveries: consumed };
        case 'reset-log':
          consumed.length = 0;
          return { reset: true };
        case 'mode': {
          const next = url.searchParams.get("mode");
          if (next === "fail" || next === "ack" || next === "hold") mode = next;
          return { mode };
        }
        case 'release': {
          if (heldResolve !== null) {
            heldResolve();
            heldResolve = null;
          }
          return { released: true };
        }
        default:
          return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
