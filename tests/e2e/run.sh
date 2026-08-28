#!/usr/bin/env bash
# Start Postgres + three BeanQueue workers, then run the e2e harness.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT/tests/e2e/compose.yaml"
PROJECT="bqe2e"

if docker info >/dev/null 2>&1; then
  DOCKER=(docker)
elif sudo docker info >/dev/null 2>&1; then
  DOCKER=(sudo docker)
else
  echo "Docker daemon is not running (need docker or sudo docker)." >&2
  exit 1
fi

COMPOSE=("${DOCKER[@]}" compose -f "$COMPOSE_FILE" --project-name "$PROJECT")

cleanup() {
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" build
"${COMPOSE[@]}" up -d postgres migrate worker-a worker-b worker-c

echo "Waiting for workers to become healthy..."
for _ in $(seq 1 60); do
  statuses=""
  ready=1
  for name in bqe2e-worker-a bqe2e-worker-b bqe2e-worker-c; do
    status="$("${DOCKER[@]}" inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$name" 2>/dev/null || echo missing)"
    statuses="$statuses $name=$status"
    if [[ "$status" != "healthy" ]]; then
      ready=0
    fi
  done
  if [[ "$ready" -eq 1 ]]; then
    echo "Workers healthy:$statuses"
    break
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "Workers did not become healthy:$statuses" >&2
  "${COMPOSE[@]}" logs --no-color || true
  cleanup
  exit 1
fi

set +e
"${COMPOSE[@]}" run --rm --no-deps tester
code=$?
set -e

if [[ "$code" -ne 0 ]]; then
  echo
  echo "===== compose logs (failure, last 120 lines per service) ====="
  "${COMPOSE[@]}" logs --no-color --timestamps --tail=120 || true
fi

cleanup
exit "$code"
