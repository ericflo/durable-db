#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
binary=${DURABLE_DB_TEST_BINARY:-$repo/target/debug/durable-db}
port=${DURABLE_DB_TEST_PORT:-17654}
tmp=$(mktemp -d)
pid=''
cleanup() { if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
"$binary" serve --data "$tmp/data" --listen "127.0.0.1:$port" > "$tmp/server.log" 2>&1 &
pid=$!
n=0
until curl -fsS "http://127.0.0.1:$port/health/ready" >/dev/null 2>&1; do
  n=$((n + 1)); [ "$n" -lt 100 ] || { cat "$tmp/server.log"; exit 1; }; sleep 0.1
done
"$binary" token --key-file "$tmp/data/admin.key" --admin > "$tmp/token"
export DURABLE_DB_TEST_URL="http://127.0.0.1:$port"
export DURABLE_DB_TEST_TOKEN_FILE="$tmp/token"
cd "$repo/clients/javascript"
npm ci --no-audit --no-fund
npm --prefix "$repo/examples/web-app" ci --no-audit --no-fund
npm run format:check
npm run typecheck
npm test -- "$@"
cd "$repo"
node --test examples/web-app/storage.test.ts
node scripts/test-benchmark.mjs
node scripts/benchmark-http.mjs --urls "$DURABLE_DB_TEST_URL" \
  --token-file "$DURABLE_DB_TEST_TOKEN_FILE" --databases 4 --workers 1,4 --operations 4 \
  --binary "$binary" --output "$tmp/benchmark.json" \
  --context 'CI local HTTP correctness smoke; not a performance qualification'
