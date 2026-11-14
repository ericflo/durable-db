#!/bin/sh
# Check the TypeScript SDK, the pi-durable adapter and the example against a
# real server. The tests start and stop their own servers.
#   DURABLE_DB_BINARY  the server to test (default target/release/durable-db, then PATH)
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo/clients/javascript"
npm ci --no-audit --no-fund
npm --prefix "$repo/examples/web-app" ci --no-audit --no-fund
npm run format:check
npm run typecheck
npm test
npm --prefix "$repo/examples/web-app" test
