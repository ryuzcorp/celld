# Cello

Self-hosted, distributed **Durable Objects**.

Cello runs a Cloudflare Workers application on your own machines:
Workers, Durable Objects, KV, Queues, D1, R2, Workflows, Cron Triggers,
and static assets, deployed from the `wrangler.json` you already have.
Each object is a cell: a named server with its own SQLite database.
Cello stores the long-term state in your S3-compatible, Google Cloud
Storage, or Azure Blob Storage bucket. It needs no serving control
plane or consensus service, and an inactive cell costs almost nothing. One 8 GB node holds about 1,000 resident cells, about $0.05 each per month.

## Install

```sh
curl -fsSL https://cello.ryuz.eu/install.sh | sh
```

Then run the [Quickstart](https://cello.ryuz.eu/quickstart): a Worker
with a Durable Object on `cello dev`, then a two-node fleet that
shares one bucket. Takes about 15 minutes.

## Docs

**https://cello.ryuz.eu** — [Install](https://cello.ryuz.eu/install) ·
[Quickstart](https://cello.ryuz.eu/quickstart) ·
[Run a fleet](https://cello.ryuz.eu/guides/run-fleet) ·
[Services](https://cello.ryuz.eu/services/workers) ·
[Changelog](https://cello.ryuz.eu/changelog)

## Based on celld

Cello is a renamed continuation of [celld](https://github.com/denoland/celld)
by Deno Land Inc. — [what Cello adds over
upstream](https://cello.ryuz.eu/project/what-cello-adds) documents every
improvement. Cello reads celld-written data; stop all celld nodes first
(a mixed fleet is unsupported), rename `CELLD_*` variables to `CELLO_*`,
and start Cello. See [Upgrading to
0.2.0](https://cello.ryuz.eu/guides/upgrade).

## License

[Apache-2.0](LICENSE); see [NOTICE](NOTICE).
