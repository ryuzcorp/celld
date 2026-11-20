---
title: Workflows
description: Run durable multi-step Workflows as cells, with replay-safe steps, sleeps, events, and retries.
---

A Workflow is a durable function that runs as a named sequence of steps, sleeps, and events. Cello runs each instance as a [cell](../concepts/durability.md): exactly one node serves it, and it survives node failures.

## Example

The full runnable version is in [`examples/workflow`](https://github.com/ryuzcorp/cello/tree/main/examples/workflow).

<!-- cello-example: workflow -->

Declare the binding in `wrangler.json`:
{
  "workflows": [
    { "binding": "REPORTS", "name": "report-builder", "class_name": "ReportBuilder" }
  ]
}
```

Extend `WorkflowEntrypoint` and run each unit of work inside `step.do()`:

```js
import { WorkflowEntrypoint } from "cloudflare:workers";

export class ReportBuilder extends WorkflowEntrypoint {
  async run(event, step) {
    return await step.do("build report", async () => {
      const response = await fetch(event.payload.url);
      if (!response.ok) throw new Error(`source answered ${response.status}`);
      const text = await response.text();
      return { bytes: text.length };
    });
  }
}
```

Create an instance through the binding and poll its status:

```js
const instance = await env.REPORTS.create({ params: { url: "https://example.com" } });
const status = await env.REPORTS.get(instance.id).status();
```

## Use the API

- A class extends `WorkflowEntrypoint` and implements `run(event, step)`. `event.payload` holds the parameters, and the return value becomes the `output` field of `status()`.
- `step.do(name, callback)` runs a durable step, and it retries the callback on failure.
- `step.sleep(name, duration)` and `step.sleepUntil(name, timestamp)` stop the instance until a deadline.
- `step.waitForEvent(name, options)` stops the instance until a matching event arrives. It times out after 24 hours by default. An event that arrives before the instance reaches its wait step is buffered.
- `env.MY_WORKFLOW.create()`, `createBatch()`, and `get(id)` return an instance. `deleteBatch()` deletes several instances.
- An instance has `status()`, `sendEvent()`, `pause()`, `resume()`, `restart()`, `terminate()`, and `delete()`.

Each time an instance makes progress, Cello calls `run()` again from the first line. A finished step returns its stored result and does not run its callback again. Put each subrequest, each side effect, and each value that must stay stable inside a `step.do()` callback, as the [rules of Workflows](https://developers.cloudflare.com/workflows/build/rules-of-workflows/) require. A step whose result did not commit before a node failure runs again, so a step callback must tolerate a second attempt.

`run()` can await work that is not a step, but a replay cannot resume that await. Cello fails the instance when such work keeps `run()` pending for 60 seconds while no step runs or waits.

Keep steps short enough that one drive makes progress. An instance whose steps sum past `CELLO_HANDLER_BUDGET_S` does not stall: each alarm-handler drive runs what fits in the budget, the expired handler counts as a failed attempt that does not consume the alarm retry limit, and the retried alarm replays the ledger and continues where the expired drive left off. A single step that outlasts the budget never completes.

A sleep, a wait, and a pending retry each store their deadline, so a crash or a slow replay cannot move the deadline. `status()` reports `waiting` for all three. A waiting instance holds no isolate: its cell hibernates when the next deadline is further away than the near-alarm residency window. That window is one hour, and `CELLO_ALARM_RESIDENT_MS` changes it.

`step.do()` uses the Cloudflare retry defaults when the call supplies no `retries` object: 5 retries, a 10-second delay, exponential backoff, and a 10-minute timeout for one attempt. Cello sets no limit on concurrent instances; fleet memory and the resident-cell cap bound it.

## Know the differences from Cloudflare

| Area | Cello behavior |
| --- | --- |
| Retention | Cello retains a successful or failed instance for 30 days by default. Each duration in the `retention` option can be at most 30 days. |
| Placement | `locationHint` accepts the Cloudflare values, but fleet ownership selects the cell location. |
| Step budget | Non-step work cannot remain pending for more than 60 seconds. A step result, an event payload, and the workflow parameters each have a 1 MiB limit. |
| Unavailable | Rollback, a sensitive step result, and a `ReadableStream` step result are unavailable. A `workflows` entry cannot carry `schedules`, `limits`, or a `script_name` that names another script. The Workflows REST API and the `wrangler workflows` commands are unavailable; drive an instance through the binding. |

The [Cloudflare compatibility](../reference/cloudflare-compat.md#services) page lists the runtime APIs and the unsupported services.

## Operate it

You operate a Workflow through its binding: create, poll `status()`, and `sendEvent()` from a Worker or from `cello dev`. No CLI manages instances.

- Cello runs `step.do()` retries, sleeps, waits, and deadlines on the node that owns the instance cell. A move to another node preserves every deadline.
- Tune the handler budget with `CELLO_HANDLER_BUDGET_S` and the hibernation window with `CELLO_ALARM_RESIDENT_MS`. Both are node settings; set them on every node.
