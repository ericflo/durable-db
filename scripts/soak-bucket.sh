#!/bin/sh
# Many databases written for a long time on a bucket, and then deleted.
#
# A community build of MinIO in a container held to 4 GiB and four CPUs. One
# server with the flags of the Kubernetes manifest, in a systemd scope
# limited to 1 GiB without swap and four CPUs: if it passes the limit it is
# killed, and this script fails. One client for each database writes 1 KiB
# values that do not compress. Then each database is deleted, one after
# another. Printed: the load's result; the most memory the server used and
# what it used from the third minute on; how many purges failed; how the
# deletions went; and what the server logged of a store that refuses, with
# how many requests it failed without sending them. The server may not
# panic, and no round of its housekeeping may run for thirty seconds.
#
# The store is one process on the machine that loads it. What it does when
# it has much to list is its own, and is why this is run: it is not what
# Backblaze B2 does.
#
#   scripts/soak-bucket.sh
#   DURABLE_DB       the binary (default durable-db)
#   SOAK_SECONDS     how long the databases are written (default 1200)
#   SOAK_DATABASES   how many (default 256)
#   SOAK_DIR         where the server keeps its cache (default: a temporary directory)
#   STORE_PORT       the port the store is given (default 19300)
#   SOAK_PORT        the server's port (default 17782)
#
# Needs Docker and curl 7.75 or later.
set -eu
bin=$(command -v "${DURABLE_DB:-durable-db}")
seconds=${SOAK_SECONDS:-1200}
databases=${SOAK_DATABASES:-256}
endpoint="http://127.0.0.1:${STORE_PORT:-19300}"
url="http://127.0.0.1:${SOAK_PORT:-17782}"
dir=$(mktemp -d -p "${SOAK_DIR:-${TMPDIR:-/tmp}}" durable-db-bucket.XXXXXX)
container=durable-db-bucket-$$
server='' sampler=''
cleanup() {
    for pid in $sampler $server; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    docker rm -f -v "$container" >/dev/null 2>&1 || true
    rm -rf "$dir"
}
trap cleanup EXIT INT TERM

random() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
access="local$(random 6)"
secret=$(random 20)
signed() {
    curl -fsS -m 20 --aws-sigv4 "aws:amz:us-east-1:s3" --user "$access:$secret" "$@"
}
docker run -d --rm --name "$container" --memory 4g --cpus 4 -p "${endpoint#http://}:9000" \
    -e MINIO_ROOT_USER="$access" -e MINIO_ROOT_PASSWORD="$secret" \
    pgsty/minio:RELEASE.2026-08-04T00-00-00Z server /data >/dev/null
tries=0
until signed "$endpoint/" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 60 ] || { echo "the store did not start"; exit 1; }
    sleep 0.5
done
signed -o /dev/null -X PUT "$endpoint/plain"

random 32 >"$dir/admin.key"
chmod 600 "$dir/admin.key"
export DURABLE_DB_BUCKET=plain DURABLE_DB_ENDPOINT="$endpoint" DURABLE_DB_REGION=us-east-1 \
    DURABLE_DB_PREFIX="soak-$(random 4)" AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret"
systemd-run --user --scope -q -p MemoryMax=1G -p MemorySwapMax=0 -p CPUQuota=400% \
    "$bin" serve --listen "${url#http://}" --key-file "$dir/admin.key" \
    --cache-dir "$dir/cache" --cache-dir-bytes 1073741824 \
    --cache-bytes 201326592 --memtable-bytes 100663296 \
    --body-bytes 134217728 --result-bytes 134217728 --max-connections 1024 \
    >"$dir/server.log" 2>&1 &
server=$!
tries=0
until curl -fs "$url/readyz" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 300 ] || { echo "the server did not start"; cat "$dir/server.log"; exit 1; }
    sleep 0.1
done
alive() {
    kill -0 "$server" 2>/dev/null || { echo "the server was killed"; tail -5 "$dir/server.log"; exit 1; }
}
echo "the server runs $("$bin" --version) on MinIO; $databases databases are written for $seconds s"

# Seconds since the start and the server's memory in MiB, every ten seconds.
began=$(date +%s)
while sleep 10; do
    echo "$(($(date +%s) - began)) $(awk '/^VmRSS/ { print int($2 / 1024) }' "/proc/$server/status" 2>/dev/null)"
done >"$dir/memory" &
sampler=$!
timeout $((seconds + 300)) "$bin" bench --url "$url" --key-file "$dir/admin.key" \
    --seconds "$seconds" --workload write --clients "$databases" --databases "$databases" \
    --value-bytes 1024 --random-values --keep 2>/dev/null | tail -1
alive
kill "$sampler"
sampler=''
awk -v most="$(awk '/^VmHWM/ { print int($2 / 1024) }' "/proc/$server/status")" '
    $1 >= 180 && NF == 2 { if (!low || $2 < low) low = $2; if ($2 > high) high = $2 }
    END { printf "memory: %d MiB at most, and from the third minute between %d and %d MiB, under a limit of 1024 MiB\n", most, low, high }
' "$dir/memory"
curl -fs "$url/metrics" | awk '
    /^durable_db_storage_seconds_count\{op="purge"\}/ { purges = $2 }
    /^durable_db_storage_failures_total\{op="purge"\}/ { failed = $2 }
    END { printf "purges while they were written: %d, of which %d failed\n", purges, failed }'

"$bin" list --url "$url" --key-file "$dir/admin.key" >"$dir/names"
failed=0 slowest=0
began=$(date +%s)
while read -r name; do
    from=$(date +%s%N)
    "$bin" delete "$name" --url "$url" --key-file "$dir/admin.key" >/dev/null 2>>"$dir/delete.err" ||
        failed=$((failed + 1))
    took=$((($(date +%s%N) - from) / 1000000))
    [ "$took" -le "$slowest" ] || slowest=$took
done <"$dir/names"
alive
echo "databases deleted: $(wc -l <"$dir/names") in $(($(date +%s) - began)) s, of which $failed failed; the slowest took $slowest ms"
[ "$failed" -eq 0 ] || sort "$dir/delete.err" | uniq -c | sort -rn | head -3
echo "what the server said of it: $(grep -c 'the store refuses' "$dir/server.log") times that the store refuses, and $(grep -c 'was not sent' "$dir/server.log") times that it failed for a request it did not send"
kill "$server"
wait "$server" 2>/dev/null || true
server=''
if grep -q 'panicked at' "$dir/server.log"; then
    grep -A1 'panicked at' "$dir/server.log" | head -6
    echo 'THE SERVER PANICKED' >&2
    exit 1
fi
if grep -q 'has not ended' "$dir/server.log"; then
    grep 'has not ended' "$dir/server.log" | tail -1
    echo 'A ROUND OF HOUSEKEEPING DID NOT END IN TIME' >&2
    exit 1
fi
