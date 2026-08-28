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
"${COMPOSE[@]}" up --build -d postgres migrate worker-a worker-b worker-c

set +e
"${COMPOSE[@]}" run --rm --no-deps tester
code=$?
set -e

if [[ "$code" -ne 0 ]]; then
  echo
  echo "===== compose logs (failure) ====="
  "${COMPOSE[@]}" logs --no-color --timestamps || true
fi

cleanup
exit "$code"
