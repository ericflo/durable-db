#!/bin/sh
# Run durable-db on S3 servers other than Backblaze B2, each in a container on
# this machine held to 1 GiB and two CPUs:
#
#   minio    a community build of MinIO: a bucket that keeps versions and one
#            that does not
#   rustfs   RustFS: likewise
#   garage   Garage, which keeps no versions and has no request that lists them
#
# On each bucket the bucket tests run. Then a ledger runs through three
# servers on the store while one is killed, one is stopped and one is killed
# again, each replaced at once. The ledger must verify: no transfer lost,
# none applied twice.
#
#   scripts/test-stores.sh [minio|rustfs|garage ...]    default: all three
#   LEDGER_SECONDS   how long the ledger runs (default 75)
#   STORE_PORT       the port the store is given (default 19000)
#   LEDGER_PORT      first of three ports for the servers (default 17761)
#
# Needs Docker, curl 7.75 or later, and target/release/durable-db
# (cargo build --release).
set -eu
cd "$(dirname "$0")/.."
bin=${DURABLE_DB_BIN:-$PWD/target/release/durable-db}
[ -x "$bin" ] || { echo "no $bin: run cargo build --release" >&2; exit 1; }
seconds=${LEDGER_SECONDS:-75}
port=${STORE_PORT:-19000}
base=${LEDGER_PORT:-17761}
endpoint="http://127.0.0.1:$port"
dir=$(mktemp -d "${TMPDIR:-/tmp}/durable-db-stores.XXXXXX")
container=durable-db-store-$$
pid_0='' pid_1='' pid_2=''
cleanup() {
    for pid in $pid_0 $pid_1 $pid_2; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    docker rm -f -v "$container" >/dev/null 2>&1 || true
    rm -rf "$dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

random() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
signed() {
    curl -fsS -m 20 --aws-sigv4 "aws:amz:$region:s3" --user "$access:$secret" "$@"
}
contain() { # image and its arguments
    docker run -d --rm --name "$container" --memory 1g --cpus 2 "$@" >/dev/null
}
until_ready() { # a command that succeeds once the store answers
    tries=0
    until "$@" >/dev/null 2>&1; do
        tries=$((tries + 1))
        [ "$tries" -lt 60 ] || { echo "the store did not start"; docker logs "$container" | tail -5; exit 1; }
        sleep 0.5
    done
}
make_buckets() { # one that keeps versions, one that does not
    signed -o /dev/null -X PUT "$endpoint/versions"
    signed -o /dev/null -X PUT "$endpoint/plain"
    signed -o /dev/null -X PUT -H 'Content-Type: application/xml' \
        --data '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>' \
        "$endpoint/versions?versioning"
    buckets='versions plain'
}

begin_minio() {
    contain -p "127.0.0.1:$port:9000" -e MINIO_ROOT_USER="$access" -e MINIO_ROOT_PASSWORD="$secret" \
        pgsty/minio:RELEASE.2026-08-04T00-00-00Z server /data
    until_ready signed "$endpoint/"
    make_buckets
}
begin_rustfs() {
    contain -p "127.0.0.1:$port:9000" -e RUSTFS_ACCESS_KEY="$access" -e RUSTFS_SECRET_KEY="$secret" \
        rustfs/rustfs:1.0.5
    until_ready signed "$endpoint/"
    make_buckets
}
begin_garage() {
    region=garage
    cat >"$dir/garage.toml" <<EOF
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$(random 32)"
[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
EOF
    chmod 644 "$dir/garage.toml"
    contain -p "127.0.0.1:$port:3900" -v "$dir/garage.toml:/etc/garage.toml:ro" dxflrs/garage:v2.5.0
    garage() { docker exec "$container" /garage "$@" 2>/dev/null; }
    until_ready garage status
    garage layout assign -z local -c 1G "$(garage status | awk '/^[0-9a-f]{16}/ { print $1; exit }')" >/dev/null
    garage layout apply --version 1 >/dev/null
    garage bucket create plain >/dev/null
    garage key create durable-db >"$dir/garage.key"
    access=$(awk -F': *' 'tolower($1) ~ /key id/ { print $2; exit }' "$dir/garage.key")
    secret=$(awk -F': *' 'tolower($1) ~ /secret key/ { print $2; exit }' "$dir/garage.key")
    garage bucket allow --read --write --owner plain --key durable-db >/dev/null
    buckets='plain'
}

start() { # server number
    listen="127.0.0.1:$((base + $1))"
    "$bin" serve --listen "$listen" --key-file "$dir/admin.key" \
        --peers "$peers" --advertise "$listen" >>"$dir/server-$1.log" 2>&1 &
    eval "pid_$1=$!"
    tries=0
    until curl -fs "http://$listen/readyz" >/dev/null 2>&1; do
        tries=$((tries + 1))
        [ "$tries" -lt 300 ] || { echo "server $1 did not start"; cat "$dir/server-$1.log"; exit 1; }
        sleep 0.1
    done
}
stop() { # server number, signal
    eval "pid=\$pid_$1"
    kill "-$2" "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    eval "pid_$1=''"
}
count() { # metric: its sum over the three servers
    for n in 0 1 2; do
        curl -s -m 2 "http://127.0.0.1:$((base + n))/metrics" || true
    done | awk -v metric="$1" '$1 == metric { sum += $2 } END { print sum + 0 }'
}
ledger() { # bucket
    random 32 >"$dir/admin.key"
    chmod 600 "$dir/admin.key"
    peers="127.0.0.1:$base,127.0.0.1:$((base + 1)),127.0.0.1:$((base + 2))"
    urls="http://127.0.0.1:$base,http://127.0.0.1:$((base + 1)),http://127.0.0.1:$((base + 2))"
    export DURABLE_DB_BUCKET="$1" DURABLE_DB_ENDPOINT="$endpoint" DURABLE_DB_REGION="$region" \
        DURABLE_DB_PREFIX="ledger-$(random 4)" AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret"
    start 0
    start 1
    start 2
    timeout $((seconds + 240)) "$bin" bench --workload ledger --url "$urls" \
        --key-file "$dir/admin.key" --clients 16 --databases 8 \
        --seconds "$seconds" >"$dir/ledger.out" 2>&1 &
    bench=$!
    sleep $((seconds / 5)); stop 1 KILL; start 1
    sleep $((seconds / 5)); stop 2 TERM; start 2
    sleep $((seconds / 5)); stop 0 KILL; start 0
    wait "$bench" || true
    tail -1 "$dir/ledger.out"
    grep -q '^ledger verified' "$dir/ledger.out" || { echo 'THE LEDGER DID NOT VERIFY' >&2; exit 1; }
    # Databases that fall idle flush and collect five seconds later.
    sleep 8
    echo "purges by the servers running at the end: $(count 'durable_db_storage_seconds_count{op="purge"}'), failed: $(count 'durable_db_storage_failures_total{op="purge"}')"
    for n in 0 1 2; do stop "$n" TERM; done
    if grep -q 'panicked at' "$dir"/server-*.log; then
        grep -h -A1 'panicked at' "$dir"/server-*.log | head -6
        echo 'A SERVER PANICKED' >&2
        exit 1
    fi
    rm -f "$dir"/server-*.log
}

for store in ${*:-minio rustfs garage}; do
    region=us-east-1
    access="local$(random 6)"
    secret=$(random 20)
    case $store in
    minio | rustfs | garage) "begin_$store" ;;
    *) echo "unknown store: $store" >&2; exit 1 ;;
    esac
    for bucket in $buckets; do
        echo "== $store, bucket '$bucket': the bucket tests"
        DDB_S3_ENDPOINT="$endpoint" DDB_S3_REGION="$region" DDB_S3_BUCKET="$bucket" \
            DDB_S3_ACCESS_KEY_ID="$access" DDB_S3_SECRET_ACCESS_KEY="$secret" \
            cargo test --test bucket -- --nocapture >"$dir/bucket.out" 2>&1 || { cat "$dir/bucket.out"; exit 1; }
        # A store that does not take the request that removes many versions is named.
        grep -e '^test result' -e 'refused the request' "$dir/bucket.out"
    done
    echo "== $store, bucket '${buckets%% *}': a ledger through three servers for $seconds s"
    ledger "${buckets%% *}"
    docker rm -f -v "$container" >/dev/null
done
