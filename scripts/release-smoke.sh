#!/usr/bin/env bash
# Modified by Ryuz Corp (Cello): release smoke gate.
#
# Runs the release image and drives the consumer-facing shapes that have
# broken in shipped images before: a long-lived SSE body out of a
# TransformStream, a body that never produces, WebSocket messages at and
# past the 32 MiB cap, an 8 MiB tee(), and a backpressured producer.
#
# Usage: scripts/release-smoke.sh IMAGE
#
#   IMAGE                 what to run, for example
#                         ghcr.io/ryuzcorp/cello:0.2.3
#                         ghcr.io/ryuzcorp/cello:0.2.3@sha256:<digest>
#                         cello-releasesmoke:local
#   SMOKE_QUICK=1         shorten the SSE span to 90 s (local iteration)
#   SMOKE_SSE_SECONDS=N   set the SSE span explicitly (default 300)
#   SMOKE_DOCKER_GROUP    run docker through `sg GROUP -c` (for a host where
#                         your shell lacks the group); unset runs docker
#                         directly
#
# Requirements: Docker, curl, and Node.js 24 or newer on the host (the
# client uses only built-in modules). The container is removed on every
# exit path.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE="$SCRIPT_DIR/release-smoke"
CONTAINER_PORT=9876

usage() {
  sed -n '9,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

die() {
  printf 'release-smoke: %s\n' "$*" >&2
  exit 2
}

# Plain `docker` unless SMOKE_DOCKER_GROUP names a group this shell lacks:
# then `sg` grants the group for the invocation, so a workstation rule stays
# out of the script's defaults.
dock() {
  if [ -n "${SMOKE_DOCKER_GROUP:-}" ]; then
    sg "$SMOKE_DOCKER_GROUP" -c "$(printf '%q ' docker "$@")"
  else
    docker "$@"
  fi
}

if [ "$#" -ne 1 ]; then
  usage >&2
  exit 2
fi
readonly IMAGE="$1"

command -v node >/dev/null 2>&1 ||
  die 'Node.js 24 or newer is required on the host (node not found)'
node_major="$(node -p 'process.versions.node.split(".")[0]')"
if [ "$node_major" -lt 24 ]; then
  die "Node.js 24 or newer is required on the host (found $(node --version))"
fi
command -v curl >/dev/null 2>&1 || die 'curl is required on the host'
command -v docker >/dev/null 2>&1 || die 'docker is required on the host'
if [ -n "${SMOKE_DOCKER_GROUP:-}" ]; then
  command -v sg >/dev/null 2>&1 ||
    die 'sg is required when SMOKE_DOCKER_GROUP is set'
fi

NAME="cello-releasesmoke-$(date +%s)-$$"

# Invoked by the EXIT/INT/TERM traps.
# shellcheck disable=SC2329
cleanup() {
  dock rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

printf 'release smoke gate\n  image: %s\n' "$IMAGE"
cello_version="$(dock run --rm "$IMAGE" --version 2>&1)" ||
  die "the image does not run 'cello --version': $cello_version"
printf '  %s\n\n' "$cello_version"

dock run -d --name "$NAME" --label cello-release-smoke=1 \
  -p "127.0.0.1::$CONTAINER_PORT" \
  -v "$FIXTURE:/app:ro" \
  --tmpfs /tmp/cello-state \
  "$IMAGE" dev /app --host 0.0.0.0 --port "$CONTAINER_PORT" \
  --state /tmp/cello-state --no-watch >/dev/null

published="$(dock port "$NAME" "$CONTAINER_PORT/tcp" | head -n 1 | awk -F: '{print $NF}')"
[ -n "$published" ] || {
  dock logs "$NAME" >&2 || true
  die 'the container did not publish a Worker port'
}
printf '  Worker on http://127.0.0.1:%s (container %s)\n\n' "$published" "$NAME"

# The Worker is ready when /health answers; a boot failure is reported with
# the container's own log rather than a bare timeout.
ready=0
for _ in $(seq 1 180); do
  if ! dock ps -q --filter "name=^${NAME}$" | grep -q .; then
    dock logs "$NAME" >&2 || true
    die 'the container exited while the Worker was booting'
  fi
  if curl -fsS --max-time 2 -o /dev/null "http://127.0.0.1:$published/health"; then
    ready=1
    break
  fi
  sleep 1
done
[ "$ready" -eq 1 ] || {
  dock logs "$NAME" >&2 || true
  die 'the Worker did not answer /health within 180 s'
}

set +e
node "$FIXTURE/client.mjs" "http://127.0.0.1:$published"
status=$?
set -e

if [ "$status" -ne 0 ]; then
  printf '\ncontainer log (tail):\n' >&2
  dock logs --tail 40 "$NAME" >&2 || true
fi
exit "$status"
