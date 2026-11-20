---
title: Cron Triggers
description: Run a Worker on a cron schedule, exactly once per occurrence across the fleet.
---

A Cron Trigger runs a Worker on a schedule. Cello keeps the schedule of a script in one reserved [cell](../concepts/durability.md), so a fleet runs each occurrence once.

## Example

The full runnable version is in [`examples/cron`](https://github.com/ryuzcorp/cello/tree/main/examples/cron).

<!-- cello-example: cron -->

Declare the schedule in `wrangler.json`:
{
  "triggers": { "crons": ["* * * * *"] }
}
```

Export a `scheduled` handler beside `fetch`:

```js
export default {
  async fetch() {
    return new Response("The cron runs once a minute.\n");
  },
  async scheduled(controller, env, ctx) {
    console.log("cron", controller.scheduledTime);
  },
};
```

Run it with `cello dev` and watch one line per minute in the node log:

```sh
cello dev --logs --port 9876
```

At each minute boundary the log shows `cell_console: cron <scheduledTime>`, where the number is the occurrence in milliseconds.

## Use the API

A Worker exports a `scheduled(controller, env, ctx)` handler.

- `controller.cron` holds the expression text.
- `controller.scheduledTime` holds the occurrence in milliseconds.
- `controller.noRetry()` stops the retries of a failed occurrence.
- `ctx.waitUntil()` extends the invocation. Cello drains this work before the next invocation.

A project declares its schedules in a `triggers.crons` array. `cello deploy` fails on a malformed expression, and on `triggers.crons` without `main`. Cello sets no limit on the number of expressions.

Cello reads the Cloudflare cron dialect. An expression has five fields: minute, hour, day of month, month, and day of week. The resolution is one minute, and the zone is UTC. Weekdays are [numbered 1 to 7 from Sunday](https://developers.cloudflare.com/workers/configuration/cron-triggers/), so `1-5` means Sunday to Thursday.

Each field takes `*`, a value, an `a-b` range, an `a,b` list, and a `/n` step. The month and day-of-week fields also take three-letter names in any case, such as `JAN` and `MON`. The day-of-month field takes `L`, `L-<n>`, `LW`, `L-<n>W`, and `<d>W`, and the day-of-week field takes `<dow>L` and `<dow>#<n>`. When both day fields are restricted, they form a union, so `0 0 1 * MON` fires on the first day of each month and on every Monday. Only a literal `*` leaves a day field unrestricted.

Cello refuses a descending range such as `SAT-SUN` and a list that contains `*`, such as `1,*`. Cloudflare accepts both with surprising results. A step cannot exceed the width of its field, so `*/60` fails in the minute field.

`controller.scheduledTime` holds the scheduled occurrence and not the start of the attempt, so a late run and a retry name the same minute. Cello runs the handlers of one script one at a time and drains the `waitUntil()` work of each before the next. A handler that takes longer than the interval delays the next occurrence. Move long work into a [queue](queues.md) or a [Workflow](workflows.md).

A handler that throws retries after 4 seconds, and the delay doubles after each further failure. Cello gives up after six failures, or at the next occurrence, which never waits for a retry, so a schedule faster than the backoff never retries. `controller.noRetry()` stops the retries. An ownership move loses a pending retry.

After downtime, Cello runs one missed occurrence and skips the rest. Read the interval from `controller.scheduledTime` to catch up in application code.

## Know the differences from Cloudflare

| Area | Cello behavior |
| --- | --- |
| Dialect | Cello rejects a descending range such as `SAT-SUN` and a list that contains `*`. |
| Delivery | Cello runs one handler for each occurrence across the fleet. After downtime, it runs one missed occurrence and skips the rest. |
| Serialization | Cello serializes the handlers for one script. It retries a failure until the next occurrence unless the handler calls `noRetry()`. |
| Service bindings | A `triggers.crons` entry in a service-binding target never runs, and Cello logs a warning when it loads such a target. |

The [Cloudflare compatibility](../reference/cloudflare-compat.md#services) page lists the runtime APIs and the unsupported services.

## Operate it

- `cello deploy` validates every expression and stops the deployment on the first malformed one. Declare the schedules on the script with `main`; a service-binding target never runs its own schedules.
- The node that owns the reserved cell runs the occurrence, and that node can change. No CLI lists or triggers occurrences; watch the node log with `cello dev --logs` locally, or your log pipeline in production.
