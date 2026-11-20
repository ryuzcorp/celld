---
title: Quickstart
description: Run a Worker with a Durable Object under cello dev, then share it across a two-node fleet on one bucket. Takes about 15 minutes.
---

You install Cello, run a counter Worker with one Durable Object locally, then deploy it to a two-node fleet that shares one bucket. You need Linux, `curl`, Node.js (for one download step), and Docker.

## Install Cello

```sh
curl -fsSL https://cello.ryuz.eu/install.sh | sh
cello --version
```

You see a version such as `cello 0.2.3`. The next step needs the self-contained esbuild binary (not a wrapper script). Download it with npm:

```sh
mkdir -p /tmp/esbuild-dl && cd /tmp/esbuild-dl
npm pack @esbuild/linux-x64 >/dev/null
tar -xzf esbuild-linux-x64-*.tgz package/bin/esbuild
install -m755 package/bin/esbuild ~/.local/bin/esbuild
export PATH="$HOME/.local/bin:$PATH"
```

## Run a Worker locally

Create a project with one Durable Object:

```sh
mkdir -p /tmp/cello-quickstart/src && cd /tmp/cello-quickstart
cat > wrangler.jsonc <<'EOF'
{
  "name": "quickstart",
  "main": "src/index.js",
  "compatibility_date": "2026-01-01",
  "durable_objects": {
    "bindings": [{ "name": "COUNTER", "class_name": "Counter" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }]
}
EOF
cat > src/index.js <<'EOF'
export class Counter {
  constructor(state) {
    this.storage = state.storage;
  }
  async fetch(request) {
    let n = (await this.storage.get("n")) ?? 0;
    n += 1;
    await this.storage.put("n", n);
    return Response.json({ n });
  }
}

export default {
  async fetch(request, env) {
    const id = env.COUNTER.idFromName("demo");
    return env.COUNTER.get(id).fetch(request);
  },
};
EOF
```

Start one node with a local object store. No Docker or cloud bucket is needed.

```sh
cello dev --port 9876
```

In another terminal, hit the Worker twice:

```sh
curl http://127.0.0.1:9876/
curl http://127.0.0.1:9876/
```

You see `{"n":1}` then `{"n":2}`. The cell keeps its SQLite state across requests. Stop `cello dev` with Ctrl-C.

## Share one bucket across two nodes

Start a local S3-compatible store with Docker. The example uses RustFS; any store that passes the [storage test](guides/run-fleet.md#configure-object-storage) works.

```sh
docker run -d --name cello-quickstart-store \
  -p 127.0.0.1:9000:9000 \
  -e RUSTFS_ACCESS_KEY=celloqs -e RUSTFS_SECRET_KEY=celloqs1234 \
  rustfs/rustfs:latest
```

Create a bucket. The example uses Python with boto3:

```sh
pip install boto3
python3 - <<'EOF'
import boto3
s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000',
    aws_access_key_id='celloqs', aws_secret_access_key='celloqs1234',
    region_name='us-east-1')
s3.create_bucket(Bucket='cello-quickstart')
print([b['Name'] for b in s3.list_buckets()['Buckets']])
EOF
```

Deploy the project from the last step to the bucket:

```sh
cd /tmp/cello-quickstart
export AWS_ACCESS_KEY_ID=celloqs AWS_SECRET_ACCESS_KEY=celloqs1234
cello deploy . --bucket s3://cello-quickstart \
  --endpoint http://127.0.0.1:9000 --region us-east-1
```

Start two nodes against the same bucket. They find each other through the bucket; there is no join command.

```sh
cello --bucket s3://cello-quickstart --endpoint http://127.0.0.1:9000 --region us-east-1 \
  --listen 127.0.0.1:8787 --internal-listen 127.0.0.1:8788 --advertise 127.0.0.1:8788
```

```sh
cello --bucket s3://cello-quickstart --endpoint http://127.0.0.1:9000 --region us-east-1 \
  --listen 127.0.0.1:8789 --internal-listen 127.0.0.1:8790 --advertise 127.0.0.1:8790
```

Curl the first node twice, then the second node, then the first node again:

```sh
curl http://127.0.0.1:8787/
curl http://127.0.0.1:8787/
curl http://127.0.0.1:8789/
curl http://127.0.0.1:8787/
```

You see `{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`. Any node serves any cell: the second node served the same counter without a copy step.

## Survive a node loss

Kill the first node (its terminal, or `kill <pid>`), then keep curling the second node:

```sh
curl http://127.0.0.1:8789/
```

The first requests fail while the lease expires. After about 15–20 seconds the second node takes the cell over and answers `{"n":5}`. No write is lost: Cello answers a write only after the data survives a failure.

## Clean up

```sh
docker rm -f cello-quickstart-store
rm -rf /tmp/cello-quickstart /tmp/esbuild-dl
```

## Next step

[Run a fleet](guides/run-fleet.md) covers real storage, deployment, local development, and node operations. [What Cello guarantees](concepts/durability.md) explains the contract this page just showed you.
