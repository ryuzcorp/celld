---
title: Queues
description: Send messages from a producer and handle them in batches in a consumer.
---

[Queues](https://developers.cloudflare.com/queues/) is a message broker. A producer Worker sends a message, and a consumer Worker receives it later in a batch. Each queue is one Cello cell. Read the [Cloudflare Queues documentation](https://developers.cloudflare.com/queues/configuration/javascript-apis/) for the API.

## Run a minimal example

This project sends one job per request and acknowledges each job in the consumer. A job for a path ending in `/fail` retries instead.

```jsonc
// wrangler.jsonc
{
  "name": "queues",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "queues": {
    "producers": [{ "binding": "JOBS", "queue": "example-jobs" }],
    "consumers": [
      {
        "queue": "example-jobs",
        "max_batch_size": 10,
        "max_batch_timeout": 5,
        "max_retries": 2,
        "dead_letter_queue": "example-jobs-dead-letter"
      },
      { "queue": "example-jobs-dead-letter" }
    ]
  }
}
```

```js
// index.js
export default {
  async fetch(request, env) {
    await env.JOBS.send({ path: new URL(request.url).pathname });
    return Response.json({ queued: true });
  },

  async queue(batch) {
    for (const message of batch.messages) {
      if (message.body.path.endsWith("/fail")) {
        message.retry({ delaySeconds: 1 });
        continue;
      }
      message.ack();
    }
  },
};
```

## Use the supported API

- `env.JOBS.send(body, { contentType, delaySeconds })` sends one message. `contentType` is `"v8"`, `"json"`, `"text"`, or `"bytes"`.
- `env.JOBS.sendBatch(messages)` sends up to 100 messages in one call.
- The consumer script exports `queue(batch, env, ctx)`. `batch.queue` names the queue, and each entry of `batch.messages` has an `id`, a `timestamp`, a `body`, and an `attempts` count.
- `message.ack()` and `message.retry({ delaySeconds })` settle one message. `batch.ackAll()` and `batch.retryAll()` settle the whole batch. The first call for a message wins.
- A handler that returns acknowledges every unsettled message. A handler that throws retries every message it did not acknowledge.

## Send from a producer

A `queues.producers` entry gives a `binding` and a `queue` name. Every producer in the fleet that names one queue writes to the same queue. Cello enforces the [Cloudflare limits](https://developers.cloudflare.com/queues/platform/limits/): a message of at most 128,000 bytes, at most 100 messages and 256,000 bytes in one `sendBatch()` call, and a `delaySeconds` of at most 86,400. A producer entry can set `delivery_delay` as the default delay.

`send()` resolves after the queue cell commits the message. The cell accepts at most 256 producer calls at a time, and it refuses a further call with a cell overload error that the producer retries.

## Consume in batches

A `queues.consumers` entry names the `queue`. Only one script can consume a queue, so a deployment in which two scripts consume one queue fails. The entry sets `max_batch_size` (10 by default, 100 at most), `max_batch_timeout` in seconds (5 by default, 60 at most), `max_retries` (3 by default), `max_concurrency` (250 at most), `retry_delay`, and `dead_letter_queue`. Cello validates each bound at deploy time. Read [Batching, retries, and delays](https://developers.cloudflare.com/queues/configuration/batching-retries/) for the meaning of each key.

Cello does not guarantee a delivery order, and it delivers a message at least once. `message.id` stays the same across a redelivery, so use it as an idempotency key. A handler that never settles its batch holds the lease until the lease expires, and Cello counts that as one failed delivery. Cello adds no exponential backoff to a retry: compute the delay from `message.attempts`. A message that passes `max_retries` moves to the `dead_letter_queue`, or Cello deletes it when the consumer names none.

## Handle the differences from Cloudflare

| Area | Cello behavior |
| --- | --- |
| Write capacity | One writer per queue. Use more queues to increase write capacity. |
| Producer pressure | The owner accepts at most 256 concurrent producer calls and refuses more; the producer retries. |
| Retention | Four days, not configurable. |
| Missing features | Pull consumers, the Queues HTTP API, dashboard controls, manual consumer attachment, R2 event notifications, and Queue event subscriptions are unavailable. |

The [Cloudflare compatibility](../reference/cloudflare-compat.md#services) page lists the runtime APIs and the unsupported services.

## Operate it

- `cello queue info|peek|purge|pause|resume|redrive QUEUE --bucket s3://NAME` inspects and operates a deployed queue: `info` reports backlog and delivery state, `peek` reads up to 10 head messages without consuming them, `purge` needs `--force`, and `redrive` returns up to 100 dead-lettered messages to their recorded sources.
- `cello dev .` delivers messages to the local consumer from local state; no bucket is needed.
