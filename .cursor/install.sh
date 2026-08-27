#!/usr/bin/env bash
# Idempotent repository bootstrap for the BeanQueue Cloud Agent environment.
# Runs after checkout: syncs Python dependencies and prepares the local
# PostgreSQL databases the application and test suite need.
set -euo pipefail

export PATH="${HOME}/.local/bin:${PATH}"

# 1. Sync Python dependencies (runtime + optional metrics + dev group) from the
#    committed uv.lock so the environment matches the project exactly.
uv sync --all-extras

# 2. Bring up the PostgreSQL cluster so we can provision roles and databases.
sudo pg_ctlcluster 16 main start 2>/dev/null || true
for _ in $(seq 1 30); do
    if pg_isready -q; then
        break
    fi
    sleep 1
done

# 3. Provision the bq role and the bq / bq_test databases (idempotent). The
#    test suite connects to bq_test and the app defaults to bq.
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='bq'" | grep -q 1; then
    sudo -u postgres psql -c "CREATE ROLE bq WITH LOGIN SUPERUSER;"
fi
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='bq'" | grep -q 1; then
    sudo -u postgres createdb -O bq bq
fi
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='bq_test'" | grep -q 1; then
    sudo -u postgres createdb -O bq bq_test
fi

# 4. Use trust auth for local connections (development only), matching the
#    POSTGRES_HOST_AUTH_METHOD=trust setup in docker-compose.yaml. This lets the
#    bq user connect over localhost with an empty password, as the tests expect.
HBA="/etc/postgresql/16/main/pg_hba.conf"
sudo sed -i -E 's|^(local[[:space:]]+all[[:space:]]+all[[:space:]]+)peer|\1trust|' "${HBA}"
sudo sed -i -E 's|^(host[[:space:]]+all[[:space:]]+all[[:space:]]+127\.0\.0\.1/32[[:space:]]+)scram-sha-256|\1trust|' "${HBA}"
sudo sed -i -E 's|^(host[[:space:]]+all[[:space:]]+all[[:space:]]+::1/128[[:space:]]+)scram-sha-256|\1trust|' "${HBA}"
sudo pg_ctlcluster 16 main reload
