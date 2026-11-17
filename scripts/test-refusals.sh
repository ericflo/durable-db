#!/bin/sh
# What a server sends a store that refuses it, and whether it comes back.
#
# A community build of MinIO in a container held to 1 GiB and two CPUs, and
# in front of it nginx, which passes every request on until it is told to
# refuse. Then it answers every change (PUT, POST, DELETE) as an S3 store
# that is asked too much does, `503 SlowDown` with `Retry-After: 2`, and goes
# on passing reads. A ledger runs through one server; for the middle third
# of its time the store refuses. Printed: how many changes the store refused,
# in the first second and in any second after it; how many reads it was
# sent meanwhile; how long after the refusal ended a change was taken. The
# ledger must verify: no transfer lost, none applied twice.
#
#   scripts/test-refusals.sh
#   DURABLE_DB_BIN   the server (default target/release/durable-db)
#   LEDGER_SECONDS   how long the ledger runs (default 60)
#   STORE_PORT       the port the store is given (default 19100)
#   LEDGER_PORT      the server's port (default 17771)
#
# Needs Docker and curl 7.75 or later.
set -eu
cd "$(dirname "$0")/.."
bin=${DURABLE_DB_BIN:-$PWD/target/release/durable-db}
[ -x "$bin" ] || { echo "no $bin: run cargo build --release" >&2; exit 1; }
seconds=${LEDGER_SECONDS:-60}
port=${STORE_PORT:-19100}
listen="127.0.0.1:${LEDGER_PORT:-17771}"
endpoint="http://127.0.0.1:$port"
dir=$(mktemp -d "${TMPDIR:-/tmp}/durable-db-refusals.XXXXXX")
container=durable-db-refusals-$$
server=''
cleanup() {
    [ -z "$server" ] || kill "$server" 2>/dev/null || true
    wait 2>/dev/null || true
    docker rm -f -v "$container-proxy" "$container" >/dev/null 2>&1 || true
    rm -rf "$dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

random() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
access="local$(random 6)"
secret=$(random 20)
signed() {
    curl -fsS -m 20 --aws-sigv4 "aws:amz:us-east-1:s3" --user "$access:$secret" "$@"
}

# The proxy shares the store's network: the store is at 127.0.0.1:9000 there
# and the proxy at 9001, which is the port published. A request keeps the
# Host it was signed with.
mkdir "$dir/proxy"
chmod 755 "$dir/proxy"
cat >"$dir/nginx.conf" <<'EOF'
events {}
http {
    log_format counted '$msec $request_method $status';
    access_log /proxy/access.log counted;
    client_max_body_size 0;
    server {
        listen 9001;
        location / {
            set $refuse "";
            if (-f /proxy/refuse) { set $refuse "refuse"; }
            if ($request_method ~ ^(PUT|POST|DELETE)$) { set $refuse "$refuse-change"; }
            if ($refuse = "refuse-change") {
                add_header Retry-After 2 always;
                return 503 '<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>';
            }
            proxy_pass http://127.0.0.1:9000;
            proxy_set_header Host $http_host;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
        }
    }
}
EOF
chmod 644 "$dir/nginx.conf"
docker run -d --rm --name "$container" --memory 1g --cpus 2 -p "127.0.0.1:$port:9001" \
    -e MINIO_ROOT_USER="$access" -e MINIO_ROOT_PASSWORD="$secret" \
    pgsty/minio:RELEASE.2026-08-04T00-00-00Z server /data >/dev/null
docker run -d --rm --name "$container-proxy" --network "container:$container" \
    -v "$dir/nginx.conf:/etc/nginx/nginx.conf:ro" -v "$dir/proxy:/proxy" \
    nginx:1.30.2-alpine >/dev/null
tries=0
until signed "$endpoint/" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 60 ] || { echo "the store did not start"; docker logs "$container-proxy" | tail -5; exit 1; }
    sleep 0.5
done
signed -o /dev/null -X PUT "$endpoint/plain"

random 32 >"$dir/admin.key"
chmod 600 "$dir/admin.key"
export DURABLE_DB_BUCKET=plain DURABLE_DB_ENDPOINT="$endpoint" DURABLE_DB_REGION=us-east-1 \
    DURABLE_DB_PREFIX="ledger-$(random 4)" AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret"
"$bin" serve --listen "$listen" --key-file "$dir/admin.key" >"$dir/server.log" 2>&1 &
server=$!
tries=0
until curl -fs "http://$listen/readyz" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 300 ] || { echo "the server did not start"; cat "$dir/server.log"; exit 1; }
    sleep 0.1
done
echo "the server runs $("$bin" --version); the store refuses changes from $((seconds / 3)) s to $((seconds * 2 / 3)) s of $seconds"

timeout $((seconds + 240)) "$bin" bench --workload ledger --url "http://$listen" \
    --key-file "$dir/admin.key" --clients 16 --databases 8 \
    --seconds "$seconds" >"$dir/ledger.out" 2>&1 &
bench=$!
sleep $((seconds / 3))
touch "$dir/proxy/refuse"
from=$(date +%s.%N)
sleep $((seconds / 3))
rm "$dir/proxy/refuse"
to=$(date +%s.%N)
wait "$bench" || true

awk -v from="$from" -v to="$to" '
    { change = $2 == "PUT" || $2 == "POST" || $2 == "DELETE" }
    $1 >= from && $1 < to && $3 == 503 {
        refused++
        if ($1 < from + 1) first++
        else if (++later[int($1 - from)] > most) most = later[int($1 - from)]
    }
    $1 >= from && $1 < to && !change { reads++ }
    $1 < from && change { before++ }
    $1 >= to && change && $3 < 300 && !taken { taken = $1 - to }
    END {
        printf "changes before the refusal: %d a second\n", before / (from - first_at)
        printf "changes refused in %.0f s: %d, of them %d in the first second and at most %d in a second after it\n", to - from, refused, first, most
        printf "reads sent meanwhile: %d\n", reads
        if (taken) printf "a change was taken %.1f s after the refusal ended\n", taken
        else print "no change was taken after the refusal ended"
    }
    NR == 1 { first_at = $1 }' "$dir/proxy/access.log"
echo "what the server said of it: $(grep -c 'the store refuses' "$dir/server.log") times that the store refuses, $(grep -c 'the store takes' "$dir/server.log") that it takes requests again"
tail -1 "$dir/ledger.out"
grep -q '^ledger verified' "$dir/ledger.out" || { echo 'THE LEDGER DID NOT VERIFY' >&2; exit 1; }
kill "$server"
wait "$server" 2>/dev/null || true
server=''
if grep -q 'panicked at' "$dir/server.log"; then
    grep -A1 'panicked at' "$dir/server.log" | head -6
    echo 'THE SERVER PANICKED' >&2
    exit 1
fi
