#!/usr/bin/env bash
# Per-boot startup for the BeanQueue Cloud Agent environment: ensure the local
# PostgreSQL cluster is running and accepting connections.
set -euo pipefail

sudo pg_ctlcluster 16 main start 2>/dev/null || true

for _ in $(seq 1 30); do
    if pg_isready -q; then
        echo "PostgreSQL is ready."
        exit 0
    fi
    sleep 1
done

echo "PostgreSQL did not become ready in time." >&2
exit 1
