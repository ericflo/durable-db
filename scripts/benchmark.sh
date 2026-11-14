#!/bin/sh
# Reproduce the tables in docs/performance.md against a running server.
#   DURABLE_DB_URL       server(s) to load (default http://127.0.0.1:7654)
#   DURABLE_DB_KEY_FILE  the deployment's signing key (default data/admin.key)
#   SECONDS_EACH         seconds per measurement (default 10)
set -eu
bin=${DURABLE_DB:-durable-db}
seconds=${SECONDS_EACH:-10}
run() { printf '%-34s ' "$1"; shift; "$bin" bench --seconds "$seconds" "$@" 2>/dev/null; }
run 'commit, 1 client'                 --workload write --clients 1
run 'commit, 32 clients, 1 database'   --workload write --clients 32
run 'commit, 256 clients, 1 database'  --workload write --clients 256
run 'commit, 64 clients, 64 databases' --workload write --clients 64 --databases 64
run 'commit with idempotency key, 32'  --workload write --clients 32 --idempotent
run 'strong read, 32 clients'          --workload read --clients 32
run 'strong read, 256 clients'         --workload read --clients 256
run 'session read, 32 clients'         --workload session-read --clients 32
run 'strong scan of 20, 32 clients'    --workload scan --clients 32
run 'mixed read/commit, 64 clients'    --workload mixed --clients 64 --databases 8
