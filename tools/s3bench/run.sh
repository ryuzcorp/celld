#!/usr/bin/env bash
# s3bench: reproducible S3 request-cost harness for bucket-posture work.
#
# Usage:
#   run.sh <bucket> <nodes> <agents> <life_s> <write_every_s> <idle_s> <drain_s> [ENV=VAL...]
#
#   1. starts a local RustFS container and creates <bucket> in it
#      (bucket names must be lowercase; create accepts 200 or 409),
#   2. starts a logging proxy in front of it and <nodes> Cello nodes
#      (CELLO_BIN, ports offset by OFF) pointed at the proxy,
#   3. deploys the test app from app/,
#   4. drives the workload, then runs count.py over the proxy log,
#   5. kills its proxy and nodes and removes its container on exit (trap).
#
# With HOT=1 the workload is one hot cell with <agents> concurrent writers
# hammering /hot/write for <life_s> seconds; otherwise <agents> agents each
# write /agent/<id>/write every <write_every_s> seconds for <life_s> seconds.
# DELAY_MS adds that latency to every proxied S3 request.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
APP="$HERE/app"

usage() {
  echo "usage: run.sh <bucket> <nodes> <agents> <life_s> <write_every_s> <idle_s> <drain_s> [ENV=VAL...]" >&2
  exit 2
}

[[ $# -ge 7 ]] || usage
BUCKET="$1"; NODES="$2"; AGENTS="$3"; LIFE_S="$4"; WRITE_EVERY_S="$5"; IDLE_S="$6"; DRAIN_S="$7"
shift 7

if [[ "$BUCKET" != "${BUCKET,,}" ]]; then
  echo "run.sh: bucket names must be lowercase: $BUCKET" >&2
  exit 1
fi
if [[ ! "$BUCKET" =~ ^[a-z0-9.-]{3,63}$ ]]; then
  echo "run.sh: invalid bucket name: $BUCKET" >&2
  exit 1
fi

# Release builds only by default: a debug binary distorts batching and every
# latency figure, and once made a release look 2x slower than `.10`.
if [[ -z "${CELLO_BIN:-}" ]]; then
  CELLO_BIN="$ROOT/target/release/cello"
  [[ -x "$CELLO_BIN" ]] || CELLO_BIN="$HOME/.cargo-target/cello-wt-$(basename "$ROOT")/release/cello"
fi
[[ -x "$CELLO_BIN" ]] || { echo "run.sh: no executable CELLO_BIN=$CELLO_BIN (cargo build --release -p cello first)" >&2; exit 1; }
command -v esbuild >/dev/null || { echo "run.sh: esbuild not on PATH (needed by cello deploy)" >&2; exit 1; }
command -v docker >/dev/null || { echo "run.sh: docker not on PATH" >&2; exit 1; }
python3 -c "import boto3" 2>/dev/null || { echo "run.sh: python3 boto3 missing (pip install boto3)" >&2; exit 1; }

# One harness per box: a concurrent run shares CPU and the Docker daemon,
# and skewed hot-cell latency by up to 40% in the `.11` dogfood, which looked
# like a regression. Refuse rather than measure noise; a leftover container
# from a killed run must be removed first.
if [[ "${S3BENCH_ALLOW_PARALLEL:-0}" != 1 ]]; then
  others=$(docker ps --filter name=cello-s3bench- --format '{{.Names}}' | tr '\n' ' ')
  if [[ -n "$others" ]]; then
    echo "== refusing: another s3bench run is up ($others); wait for it, remove a leftover with 'docker rm -f <name>', or set S3BENCH_ALLOW_PARALLEL=1 for counts-only runs" >&2
    exit 2
  fi
fi

OFF="${OFF:-0}"
HOT="${HOT:-0}"
DELAY_MS="${DELAY_MS:-0}"
AWS_REGION="${AWS_REGION:-us-east-1}"
# Fixed store credentials, known only to this run's container and nodes.
S3_USER="s3bench"
S3_PASS="s3bench-s3bench-01"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/s3bench.XXXXXX")"
LOG="$WORK/proxy.log"
# Reserve every port this run binds in one step, holding the sockets open
# together so the picks are distinct and nothing else can take one mid-run.
# Layout: store, proxy, then one public/private pair per node starting at
# index 2 + OFF, so the node ports always come from the reserved set too.
if [[ ! "${OFF:-0}" =~ ^[0-9]+$ ]]; then
  echo "run.sh: OFF must be a non-negative integer" >&2
  exit 1
fi
PORTS=($(python3 - "$NODES" "$OFF" <<'EOF'
import socket, sys
need = 2 + int(sys.argv[2]) + 2 * int(sys.argv[1])
socks = []
try:
    for _ in range(need):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        socks.append(sock)
    print(" ".join(str(sock.getsockname()[1]) for sock in socks))
finally:
    for sock in socks:
        sock.close()
EOF
))
STORE_PORT="${PORTS[0]}"
PROXY_PORT="${PORTS[1]}"
STORE_URL="http://127.0.0.1:$STORE_PORT"
PROXY_URL="http://127.0.0.1:$PROXY_PORT"
CONTAINER="cello-s3bench-$$"

node_pub() { echo "${PORTS[$((2 + OFF + 2 * $1))]}"; }
node_priv() { echo "${PORTS[$((2 + OFF + 2 * $1 + 1))]}"; }

NODE_PIDS=""
PROXY_PID=""
cleanup() {
  # Nodes first (they may still write through the proxy while stopping),
  # then the proxy, then the container: nothing leaks.
  for pid in $NODE_PIDS; do kill "$pid" 2>/dev/null || true; done
  deadline=$((SECONDS + 30))
  for pid in $NODE_PIDS; do
    while kill -0 "$pid" 2>/dev/null && [[ $SECONDS -lt $deadline ]]; do sleep 0.2; done
    kill -9 "$pid" 2>/dev/null || true
  done
  for pid in $NODE_PIDS; do wait "$pid" 2>/dev/null || true; done
  if [[ -n "$PROXY_PID" ]]; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
  fi
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  if [[ "${KEEP_WORK:-0}" == 1 || "$EXIT_CODE" -ne 0 ]]; then
    echo "== kept work dir: $WORK" >&2
  else
    rm -rf "$WORK"
  fi
}
EXIT_CODE=0
trap 'EXIT_CODE=$?; cleanup' EXIT

echo "== s3bench: bucket=$BUCKET nodes=$NODES agents=$AGENTS life=${LIFE_S}s every=${WRITE_EVERY_S}s idle=${IDLE_S}s drain=${DRAIN_S}s hot=$HOT delay=${DELAY_MS}ms" >&2

echo "== starting RustFS ($CONTAINER on $STORE_URL)" >&2
docker run -d --name "$CONTAINER" -p "127.0.0.1:$STORE_PORT:9000" \
  -e "RUSTFS_ACCESS_KEY=$S3_USER" -e "RUSTFS_SECRET_KEY=$S3_PASS" \
  rustfs/rustfs server /data --address :9000 >/dev/null
python3 - "$STORE_URL" "$S3_USER" "$S3_PASS" <<'EOF'
import sys, time
import boto3
url, user, password = sys.argv[1], sys.argv[2], sys.argv[3]
deadline = time.time() + 60
while True:
    try:
        boto3.client("s3", endpoint_url=url, aws_access_key_id=user,
                     aws_secret_access_key=password,
                     region_name="us-east-1").list_buckets()
        break
    except Exception:
        if time.time() > deadline:
            raise
        time.sleep(0.2)
EOF

echo "== creating bucket $BUCKET" >&2
S3_ENDPOINT="$STORE_URL" AWS_ACCESS_KEY_ID="$S3_USER" AWS_SECRET_ACCESS_KEY="$S3_PASS" \
  AWS_REGION="$AWS_REGION" python3 - "$BUCKET" <<'EOF'
import os
import sys
import boto3
import botocore
bucket = sys.argv[1]
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                   region_name=os.environ.get("AWS_REGION", "us-east-1"))
try:
    status = s3.create_bucket(Bucket=bucket)["ResponseMetadata"]["HTTPStatusCode"]
except botocore.exceptions.ClientError as error:
    status = error.response["ResponseMetadata"]["HTTPStatusCode"]
    code = error.response["Error"]["Code"]
    if status not in (200, 409) or code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
        print(f"create_bucket failed: {status} {code}", file=sys.stderr)
        sys.exit(1)
    print(f"bucket exists (create: {status} {code})")
else:
    if status not in (200, 409):
        print(f"create_bucket failed: {status}", file=sys.stderr)
        sys.exit(1)
    print(f"bucket created (create: {status})")
EOF

echo "== starting proxy 127.0.0.1:$PROXY_PORT -> $STORE_URL" >&2
touch "$LOG"
PROXY_PORT="$PROXY_PORT" UPSTREAM="127.0.0.1:$STORE_PORT" LOG="$LOG" DELAY_MS="$DELAY_MS" \
  nohup python3 "$HERE/proxy.py" >"$WORK/proxy.out" 2>&1 &
PROXY_PID=$!
python3 - "$PROXY_PORT" <<'EOF'
import socket, sys, time
port = int(sys.argv[1])
deadline = time.time() + 30
while True:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()
        break
    except OSError:
        if time.time() > deadline:
            print("proxy never listened", file=sys.stderr)
            sys.exit(1)
        time.sleep(0.2)
EOF

NODE_ENV=(
  "S3_ENDPOINT=$PROXY_URL"
  "CELLO_BUCKET=s3://$BUCKET"
  "AWS_ACCESS_KEY_ID=$S3_USER"
  "AWS_SECRET_ACCESS_KEY=$S3_PASS"
  "AWS_REGION=$AWS_REGION"
  "CELLO_DEPLOY_POLL_S=1"
)
for extra in "$@"; do
  [[ "$extra" == *=* ]] || { echo "run.sh: extra args must be ENV=VAL: $extra" >&2; exit 1; }
  NODE_ENV+=("$extra")
done

i=0
while [[ $i -lt $NODES ]]; do
  pub="$(node_pub "$i")"
  priv="$(node_priv "$i")"
  echo "== starting node $i (public 127.0.0.1:$pub internal 127.0.0.1:$priv)" >&2
  env "${NODE_ENV[@]}" "CELLO_NODE=s3bench-$i" "CELLO_WATCH=$WORK/watch-$i" \
    nohup "$CELLO_BIN" --bucket "s3://$BUCKET" --endpoint "$PROXY_URL" --region "$AWS_REGION" \
      --listen "127.0.0.1:$pub" --internal-listen "127.0.0.1:$priv" \
      --advertise "127.0.0.1:$priv" >"$WORK/node-$i.log" 2>&1 &
  NODE_PIDS="$NODE_PIDS $!"
  i=$((i + 1))
done
# Fail fast if a node died at boot (bad flags, bound port) instead of
# burning the deployment-wait timeout.
sleep 3
i=0
for pid in $NODE_PIDS; do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "run.sh: node $i exited at boot; tail of $WORK/node-$i.log:" >&2
    tail -20 "$WORK/node-$i.log" >&2
    exit 1
  fi
  i=$((i + 1))
done
PUB0="$(node_pub 0)"
echo "== deploying $APP" >&2
env "${NODE_ENV[@]}" "$CELLO_BIN" deploy "$APP" \
  --bucket "s3://$BUCKET" --endpoint "$PROXY_URL" --region "$AWS_REGION" >&2

echo "== waiting for the deployment to serve" >&2
python3 - "http://127.0.0.1:$PUB0" <<'EOF'
import sys, time, urllib.error, urllib.request
base = sys.argv[1]
deadline = time.time() + 120
while True:
    try:
        with urllib.request.urlopen(base + "/s3bench-no-such-route", timeout=5) as response:
            pass
    except urllib.error.HTTPError as error:
        # The app answers unknown paths with its own 404 body; anything
        # earlier (no deployment yet) answers differently. Unmatched paths
        # touch no storage, so polling adds no S3 traffic.
        if error.code == 404 and error.read() == b"not found":
            break
    except Exception:
        pass
    if time.time() > deadline:
        print("deployment never served", file=sys.stderr)
        sys.exit(1)
    time.sleep(0.5)
EOF

echo "== driving the workload" >&2
if [[ "$HOT" == 1 ]]; then
  python3 - "http://127.0.0.1:$PUB0" "$AGENTS" "$LIFE_S" <<'EOF'
import concurrent.futures, http.client, math, sys, threading, time
base = sys.argv[1].split("://", 1)[1]
workers, life = int(sys.argv[2]), float(sys.argv[3])
host, _, port = base.partition(":")
# One write per writer every ROUND_S, each on its own phase from a shared
# start: arrivals stay uniform (no lockstep bursts), so upload batching
# and ownership-read sharing behave like independent concurrent writers.
# 16 writers at 160 ms ~= 1500 writes in 15 s.
ROUND_S = 0.16
start = time.time()
stop = start + life
ok = 0
failed = 0
fail_lock = threading.Lock()
fail_notes = []
lat = []

def pct(xs, q):
    # Nearest-rank percentile over end-to-end turn latencies.
    if not xs:
        return 0.0
    ordered = sorted(xs)
    return ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)]

def note_failure(note):
    global failed
    with fail_lock:
        failed += 1
        if len(fail_notes) < 5:
            fail_notes.append(note)

def hammer(index):
    global ok
    conn = http.client.HTTPConnection(host, int(port), timeout=120)
    local = 0
    local_lat = []
    # Phase writers across the round so they do not fire in lockstep.
    first = start + index * (ROUND_S / workers)
    while True:
        wait_until = time.time() + ROUND_S
        if wait_until - ROUND_S >= stop:
            break
        if wait_until - ROUND_S < first:
            time.sleep(first - (wait_until - ROUND_S))
        begun = time.time()
        try:
            conn.request("GET", "/hot/write")
            response = conn.getresponse()
            response.read()
            if 200 <= response.status < 300:
                local += 1
                local_lat.append((time.time() - begun) * 1000.0)
            else:
                note_failure(f"hot/write status {response.status}")
        except Exception as error:
            note_failure(f"hot/write error {type(error).__name__}: {error}")
            try:
                conn.close()
            except Exception:
                pass
            conn = http.client.HTTPConnection(host, int(port), timeout=120)
        delay = wait_until - time.time()
        if delay > 0:
            time.sleep(delay)
    with fail_lock:
        ok += local
        lat.extend(local_lat)

with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
    list(pool.map(hammer, range(workers)))
print(f"ok turns: {ok}")
print(f"turn latency ms: n={len(lat)} mean={sum(lat)/len(lat) if lat else 0:.0f} p50={pct(lat, 0.50):.0f} p95={pct(lat, 0.95):.0f} p99={pct(lat, 0.99):.0f}")
if failed:
    print(f"failures: {failed}", file=sys.stderr)
    for note in fail_notes:
        print(f"  {note}", file=sys.stderr)
    sys.exit(1)
EOF
else
  python3 - "http://127.0.0.1:$PUB0" "$AGENTS" "$LIFE_S" "$WRITE_EVERY_S" <<'EOF'
import concurrent.futures, http.client, math, sys, threading, time
base = sys.argv[1].split("://", 1)[1]
agents, life, every = int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
host, _, port = base.partition(":")
start = time.time()
deadline = start + life
# Ceiling division: a final partial interval still gets its round (e.g.
# life=25, every=10 fires at 0, 10 and 20). The divisible case is
# unchanged: life=60, every=10 fires at 0..50, since t=60 is past deadline.
rounds = max(1, math.ceil(life / every)) if every > 0 else 1
ok = 0
fired = 0
failed = 0
fail_lock = threading.Lock()
fail_notes = []

def note_failure(note):
    global failed
    with fail_lock:
        failed += 1
        if len(fail_notes) < 5:
            fail_notes.append(note)

# One thread per agent, each with its own connection, writing every `every`
# seconds on its own phase (start + agent*every/agents): agents are
# independent concurrent clients like real Noite/Outer agents, so arrivals
# are uniform and cross-cell group commit can batch. A serial walk would
# keep at most one cell in flight and never batch.
def phase_of(agent):
    return agent * (every / agents) if every > 0 else 0

def due(agent, round_):
    # Seconds after start this agent's round is scheduled; a round is part
    # of the run only when it is scheduled within `life`.
    return round_ * every + phase_of(agent) if every > 0 else 0

def drive(agent):
    conn = http.client.HTTPConnection(host, int(port), timeout=120)
    local_ok = 0
    local_fired = 0
    local_lat = []
    for round_ in range(rounds):
        offset = due(agent, round_)
        if offset > life:
            break
        delay = start + offset - time.time()
        if delay > 0:
            time.sleep(delay)
        # A round that slipped past the deadline is not fired; `expected`
        # below still counts it, so the run reports the shortfall.
        if time.time() > deadline:
            break
        local_fired += 1
        begun = time.time()
        try:
            conn.request("GET", f"/agent/{agent}/write")
            response = conn.getresponse()
            response.read()
            if 200 <= response.status < 300:
                local_ok += 1
                local_lat.append((time.time() - begun) * 1000.0)
            else:
                note_failure(f"agent/{agent} status {response.status}")
        except Exception as error:
            note_failure(f"agent/{agent} error {type(error).__name__}: {error}")
            try:
                conn.close()
            except Exception:
                pass
            conn = http.client.HTTPConnection(host, int(port), timeout=120)
    return (local_ok, local_fired, local_lat)

def pct(xs, q):
    # Nearest-rank percentile over end-to-end turn latencies.
    if not xs:
        return 0.0
    ordered = sorted(xs)
    return ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)]

with concurrent.futures.ThreadPoolExecutor(max_workers=agents) as pool:
    results = list(pool.map(drive, range(agents)))
ok = sum(item[0] for item in results)
fired = sum(item[1] for item in results)
lat = [d for item in results for d in item[2]]
# Fixed by the schedule, not by what fired: a round truncated by the
# deadline (or by another agent's slow write) must not shrink the bar.
expected = sum(1 for agent in range(agents) for round_ in range(rounds)
               if due(agent, round_) <= life)
print(f"ok turns: {ok}")
print(f"turn latency ms: n={len(lat)} mean={sum(lat)/len(lat) if lat else 0:.0f} p50={pct(lat, 0.50):.0f} p95={pct(lat, 0.95):.0f} p99={pct(lat, 0.99):.0f}")
if failed:
    print(f"failures: {failed} (expected {expected} ok)", file=sys.stderr)
    for note in fail_notes:
        print(f"  {note}", file=sys.stderr)
    sys.exit(1)
if ok != expected:
    print(f"shortfall: {ok} ok of {expected} expected, no errors recorded",
          file=sys.stderr)
    sys.exit(1)
EOF
fi

echo "== waiting ${IDLE_S}s idle + ${DRAIN_S}s drain" >&2
sleep "$IDLE_S"
sleep "$DRAIN_S"

echo "== S3 request counts" >&2
python3 "$HERE/count.py" "$LOG"
echo "== stored cells/ objects" >&2
S3_ENDPOINT_REAL="$STORE_URL" S3_ENDPOINT="$PROXY_URL" \
  AWS_ACCESS_KEY_ID="$S3_USER" AWS_SECRET_ACCESS_KEY="$S3_PASS" AWS_REGION="$AWS_REGION" \
  python3 "$HERE/stored.py" "$BUCKET"
