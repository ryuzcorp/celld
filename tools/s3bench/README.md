# s3bench: reproducible S3 request-cost harness

Measures what one celld run costs the bucket: every request the nodes make
to the object store goes through `proxy.py`, which logs one JSON line per
request, and `count.py` classifies the log into `METHOD x key-family`
counts. `stored.py` reports the `cells/` objects and bytes left behind.

## Layout

- `proxy.py` — threaded HTTP reverse proxy in front of the S3 endpoint.
  Logs one JSON line per request: method, path, query, status, bytes
  in/out. `DELAY_MS` adds that latency to every request. Forwards the Host
  header as the client sent it, so SigV4-signed requests still verify
  upstream (the signature covers `host`; path-style addressing routes on
  the path, not the host).
- `count.py` — classifies the log into GET/PUT/LIST/DELETE/POST/HEAD × key
  family (`cells/*/ltx`, `cells/*/own.json`, `log/*/bundle`, `nodes/`,
  `fleet/capacity-v1.json`), prints per-family counts, a TOTAL line, and a
  Class A / Class B summary using R2 pricing (Class A = PUT/POST/LIST/COPY,
  Class B = GET/HEAD, free = single-key DELETE only; bulk POST ?delete is
  Class A).
- `stored.py <bucket>` — lists stored `cells/` objects and bytes per key
  family via boto3. Reads `S3_ENDPOINT_REAL` (or `S3_ENDPOINT`), `AWS_*`,
  `AWS_REGION` from the environment.
- `run.sh` — the full loop (see below).
- `app/` — minimal Worker + Durable Object (SQLite storage) app with
  `/agent/<id>/write` and `/hot/write` routes.

## Prerequisites

- `cargo build -p celld` (or set `CELLD_BIN` to the binary),
- `esbuild` on `PATH` (needed by `celld deploy`),
- `docker` (starts a `rustfs/rustfs` container, credentials via env),
- `python3` with `boto3`.

`run.sh` suspect env knobs: `CELLD_BIN`, `OFF` (port offset), `HOT=1`
(hot-cell workload), `DELAY_MS` (proxy latency), `KEEP_WORK=1` (keep the
work dir, which a successful run deletes).

## Agent workload

200 short-lived agents, 6 writes each, then eviction (`0054` numbers).
Agents are concurrent: one thread per agent, each with its own connection,
writing every `write_every_s` seconds on its own phase, so arrivals are
uniform and cross-cell group commit can batch — like independent Noite/Outer
clients. A serial walk would keep at most one cell in flight and never batch.
`DELAY_MS` emulates store latency; R2 from a VPS answers a PUT in
~100–200 ms, so `DELAY_MS=100` gives realistic batching numbers.

```sh
./run.sh s3bench-agent-1 1 200 60 10 60 120 CELLD_DURABILITY=bucket CELLD_IDLE_EVICT_S=30 CELLD_TTL_MS=60000
```

## Hot workload

One hot cell, 16 concurrent writers, 40 ms store latency:

```sh
HOT=1 DELAY_MS=40 ./run.sh s3bench-hot-1 1 16 15 0 0 10 CELLD_DURABILITY=bucket CELLD_TTL_MS=60000
```

## Notes

- Bucket names must be lowercase. Creation accepts 200 or 409 and aborts
  on any other failure.
- `run.sh` kills its proxy and nodes and removes its container on exit
  (trap); a failed run keeps its work dir and prints the path.
- The readiness poll hits an unmatched Worker route, which touches no
  storage, so it adds no S3 traffic to the measurement.
- The workload drivers count failed writes (connection errors and non-2xx
  responses, with the first details) and exit nonzero on any failure; the
  agent driver also exits nonzero when the ok count falls short of the
  rounds it fired, so a short run fails instead of printing counts as if
  it were fine.
