#!/bin/sh
# One database filled to a size, then opened by a new server, read, written
# over and deleted.
#
# One server with the flags of the Kubernetes manifest, in a systemd scope
# limited to 1 GiB without swap and four CPUs: if it passes the limit it is
# killed, and this script fails. Its store is a local directory; with
# SOAK_STORE=minio a community build of MinIO in a container held to 4 GiB
# and four CPUs; or with SOAK_STORE=bucket the bucket that DDB_S3_ENDPOINT,
# DDB_S3_BUCKET, DDB_S3_ACCESS_KEY_ID and DDB_S3_SECRET_ACCESS_KEY name (and
# DDB_S3_REGION unless the endpoint names it), under a prefix of its own.
# Every key is written once, in an order spread over the keys, with values
# that do not compress. Printed: the fill's result and how its rate changed;
# what the database holds, how much was sent to storage for it and how much
# was read back; the most memory the server used; how long a new server with
# an empty cache took to open it after the first was killed; strong reads
# from that server, twice over, since the first are of what it has in
# neither cache, and scans, each with what was read from storage for it
# and in how many requests;
# writes over keys taken at random; how long a write
# waits when the quota is then set under what the database holds, which is
# how long a merge of all of it takes; how long a third server took to open
# it after the second was stopped, with nothing left to store again; how
# long the deletion took; and how many requests to storage failed. No server
# may panic, and no round of housekeeping may run for thirty seconds.
#
# A local directory answers in a fraction of a millisecond, and MinIO here
# is one process on the machine that loads it. On those this says what a
# database of a size costs a server in memory, in bytes written and in time
# to open; it does not say how fast one is on a provider's bucket.
#
# The kernel counts what a server reads of a local directory against the
# server's limit of 1 GiB, and keeps little of a larger database in memory
# for it: the reads would then measure this machine's disk, and differ
# several times over from one run to the next. So before the reads and the
# scans this script reads the directory itself, from outside that limit,
# and the kernel has it in memory: a local directory needs as much free
# memory as the database is large.
#
# On a bucket every read of what is in neither cache is a request, and the
# reads and scans send as many at once as they have clients: give them few
# (SOAK_READ_CLIENTS) on an account that others use. The database is
# deleted at the end; the server's own few objects under the prefix stay.
#
#   scripts/soak-size.sh
#   DURABLE_DB         the binary (default durable-db)
#   SOAK_GIB           the size to fill to, in GiB (default 4)
#   SOAK_MIB           the size to fill to, in MiB, in the place of SOAK_GIB
#   SOAK_VALUE_BYTES   bytes in a value (default 4096)
#   SOAK_CLIENTS       clients that write (default 64)
#   SOAK_READ_CLIENTS  clients that read and scan (default: as many)
#   SOAK_SECONDS       how long it is read, and scanned; written over for twice that (default 60)
#   SOAK_RUNS          runs the database is to have when the first server is killed (default: as the fill left it)
#   SOAK_STORE         minio or bucket (default: a local directory)
#   SOAK_LINK          with minio, a slow link to it: UP,DOWN,RTT, bytes a second each way and a
#                      round trip in ms (scripts/slow-link.py; default: none)
#   SOAK_CACHE_MIB     the servers' disk cache, in MiB (default 1024, the Kubernetes manifest's)
#   SOAK_DIR           where to put the data (default: a temporary directory)
#   STORE_PORT         the port MinIO is given (default 19310)
#   SOAK_PORT          the server's port (default 17783)
#
# Needs four times the size in free disk on a local directory or MinIO, and
# with SOAK_STORE=minio Docker and curl 7.75 or later.
set -eu
bin=$(command -v "${DURABLE_DB:-durable-db}")
mib=${SOAK_MIB:-$((${SOAK_GIB:-4} * 1024))}
value=${SOAK_VALUE_BYTES:-4096}
clients=${SOAK_CLIENTS:-64}
readers=${SOAK_READ_CLIENTS:-$clients}
seconds=${SOAK_SECONDS:-60}
cache=${SOAK_CACHE_MIB:-1024}
keys=$((mib * 1048576 / value))
endpoint="http://127.0.0.1:${STORE_PORT:-19310}"
url="http://127.0.0.1:${SOAK_PORT:-17783}"
dir=$(mktemp -d -p "${SOAK_DIR:-${TMPDIR:-/tmp}}" durable-db-size.XXXXXX)
container=durable-db-size-$$
server='' sampler='' link=''
cleanup() {
    status=$?
    # A run that failed on a bucket does not leave gigabytes in it.
    if [ "${SOAK_STORE:-}" = bucket ] && [ "$status" -ne 0 ] && [ -n "$server" ]; then
        "$bin" delete size-0 --url "$url" --key-file "$dir/admin.key" >/dev/null 2>&1 ||
            echo "what was written may be left under the prefix ${DURABLE_DB_PREFIX:-}" >&2
    fi
    for pid in $sampler $server $link; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    [ "${SOAK_STORE:-}" != minio ] || docker rm -f -v "$container" >/dev/null 2>&1 || true
    rm -rf "$dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

random() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
random 32 >"$dir/admin.key"
chmod 600 "$dir/admin.key"
if [ "${SOAK_STORE:-}" = minio ]; then
    where='MinIO'
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
    served=$endpoint
    if [ -n "${SOAK_LINK:-}" ]; then
        up=${SOAK_LINK%%,*} rtt=${SOAK_LINK##*,}
        down=${SOAK_LINK#*,} down=${down%,*}
        python3 "$(dirname "$0")/slow-link.py" $((${STORE_PORT:-19310} + 1)) "${endpoint#http://}" \
            "$up" "$down" "$rtt" >"$dir/link" 2>&1 &
        link=$!
        until grep -q ready "$dir/link" 2>/dev/null; do
            kill -0 "$link" 2>/dev/null || { cat "$dir/link"; exit 1; }
            sleep 0.1
        done
        served="http://127.0.0.1:$((${STORE_PORT:-19310} + 1))"
        where="MinIO over a link of $up bytes a second up and $down down, $rtt ms a round trip"
    fi
    export DURABLE_DB_BUCKET=plain DURABLE_DB_ENDPOINT="$served" DURABLE_DB_REGION=us-east-1 \
        DURABLE_DB_PREFIX="soak-$(random 4)" AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret"
    set -- --key-file "$dir/admin.key"
elif [ "${SOAK_STORE:-}" = bucket ]; then
    : "${DDB_S3_ENDPOINT:?}" "${DDB_S3_BUCKET:?}" "${DDB_S3_ACCESS_KEY_ID:?}" "${DDB_S3_SECRET_ACCESS_KEY:?}"
    where='a bucket'
    export DURABLE_DB_BUCKET="$DDB_S3_BUCKET" DURABLE_DB_ENDPOINT="$DDB_S3_ENDPOINT" \
        DURABLE_DB_PREFIX="size-test-$(date +%s)" \
        AWS_ACCESS_KEY_ID="$DDB_S3_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$DDB_S3_SECRET_ACCESS_KEY"
    [ -z "${DDB_S3_REGION:-}" ] || export DURABLE_DB_REGION="$DDB_S3_REGION"
    set -- --key-file "$dir/admin.key"
else
    where='a local directory'
    mkdir "$dir/data"
    cp "$dir/admin.key" "$dir/data/admin.key"
    set -- --data "$dir/data"
fi

start() {
    systemd-run --user --scope -q -p MemoryMax=1G -p MemorySwapMax=0 -p CPUQuota=400% \
        "$bin" serve --listen "${url#http://}" "$@" \
        --cache-dir "$dir/cache" --cache-dir-bytes $((cache * 1048576)) \
        --cache-bytes 201326592 --memtable-bytes 100663296 \
        --body-bytes 134217728 --result-bytes 134217728 --max-connections 1024 \
        >>"$dir/server.log" 2>&1 &
    server=$!
    tries=0
    until curl -fs "$url/readyz" >/dev/null 2>&1; do
        tries=$((tries + 1))
        [ "$tries" -lt 300 ] || { echo "the server did not start"; cat "$dir/server.log"; exit 1; }
        sleep 0.1
    done
}
alive() {
    kill -0 "$server" 2>/dev/null || { echo "the server was killed"; tail -5 "$dir/server.log"; exit 1; }
}
bench() {
    timeout 86400 "$bin" bench --url "$url" --key-file "$dir/admin.key" --database size \
        --keys "$keys" "$@" 2>/dev/null | tail -1
    alive
}
# A read workload, and what the server fetched from storage to answer it.
reading() {
    [ -n "${SOAK_STORE:-}" ] || find "$dir/data" -type f -exec cat {} + >/dev/null 2>&1 || true
    fetched=$(metric durable_db_storage_received_bytes_total)
    asked=$(metric 'durable_db_storage_seconds_count{op="get"}')
    bench --clients "$readers" --seconds "$seconds" "$@"
    echo "    for which $(mib $(($(metric durable_db_storage_received_bytes_total) - fetched))) MiB were read from storage in $(($(metric 'durable_db_storage_seconds_count{op="get"}') - asked)) requests; the indexes of $(tables "$(info)") were then $(mib "$(metric durable_db_table_index_bytes)") MiB of the server's memory"
}
token=$("$bin" token --admin --key-file "$dir/admin.key")
info() { curl -fsS -m 60 -H "authorization: Bearer $token" "$url/v1/databases/size-0"; }
field() { sed -E "s/.*\"$1\":([0-9]+).*/\1/"; }
# The runs of what `info` printed, and its tables with them: a read of a
# key looks in one table of each run. A server before 2.1.8 does not say
# how many.
runs() { echo "$1" | sed -nE 's/.*"runs":([0-9]+).*/\1/p'; }
tables() { echo "$(echo "$1" | field tables) tables of $(runs "$1" | grep . || echo '?') runs"; }
metric() { curl -fs "$url/metrics" | awk -v name="$1" '$1 == name { print int($2) }'; }
mib() { echo $(($1 / 1048576)); }
peak() { awk '/^VmHWM/ { print int($2 / 1024) }' "/proc/$server/status"; }
failures() { curl -fs "$url/metrics" | awk '/^durable_db_storage_failures_total/ { n += $2 } END { print n + 0 }'; }

start "$@"
echo "the server runs $("$bin" --version) on $where; one database is filled with $keys keys of $value bytes, $mib MiB, by $clients clients; $readers read it; a server's disk cache is $cache MiB"

# Seconds since the start, the commit requests answered so far, and the
# runs the database has, about every ten seconds; `-` for what could not be
# read.
began=$(date +%s)
while sleep 10; do
    at=$(($(date +%s) - began))
    answered=$(metric 'durable_db_request_seconds_count{kind="commit"}')
    now=$(runs "$(info 2>/dev/null)")
    echo "$at ${answered:--} ${now:--}"
done >"$dir/commits" 2>/dev/null &
sampler=$!
bench --clients "$clients" --workload fill --max-bytes $((mib * 4 * 1048576)) --value-bytes "$value" --random-values
took=$(($(date +%s) - began))
kill "$sampler"
sampler=''
awk -v took="$took" '$2 != "-" { if (seen) { d = ($2 - last) * 10 / ($1 - then); if (low == "" || d < low) low = d; if (d > high) high = d } seen = 1; last = $2; then = $1 }
    $3 != "-" && $3 + 0 > most { most = $3 + 0 }
    END { printf "the fill took %d s; in ten seconds of it between %d and %d commit requests were answered, those told to wait among them; the database had %s runs at most\n", took, low, high, most == "" ? "?" : most }' "$dir/commits"
held=$(info)
filled=$(echo "$held" | field bytes)
sent=$(metric durable_db_storage_sent_bytes_total)
echo "the database holds $(mib "$(echo "$held" | field bytes)") MiB in $(tables "$held"); $(mib "$sent") MiB were sent to storage for it, $(awk -v s="$sent" -v w="$((keys * value))" 'BEGIN { printf "%.1f", s / w }') times what was written, and $(mib "$(metric durable_db_storage_received_bytes_total)") MiB were read back"
# A read looks in every run, and a database has most when its server is in
# a long merge that tables have been flushed behind. With SOAK_RUNS, keys
# taken at random are written over until it has that many, for ten minutes
# at most, and the server is killed then.
if [ -n "${SOAK_RUNS:-}" ]; then
    from=$(date +%s)
    timeout 600 "$bin" bench --url "$url" --key-file "$dir/admin.key" --database size --keys "$keys" \
        --clients "$clients" --workload write --value-bytes "$value" --random-values --seconds 600 \
        >/dev/null 2>&1 &
    sampler=$!
    most=0
    while kill -0 "$sampler" 2>/dev/null; do
        sleep 1
        alive
        # A server that is written this hard may not answer at once.
        now=$(info 2>/dev/null) || continue
        held=$now
        now=$(runs "$held")
        [ -n "$now" ] || { echo "this server does not say how many runs a database has"; exit 1; }
        [ "$now" -le "$most" ] || most=$now
        [ "$now" -lt "$SOAK_RUNS" ] || break
    done
    echo "written over for $(($(date +%s) - from)) s, in which it had $most runs at most, it is killed with $(tables "$held")"
fi
echo "most memory: $(peak) MiB, under a limit of 1024 MiB"

failed=$(failures)

# A new server, with nothing in memory and nothing in its disk cache.
kill -KILL "$server"
wait "$server" 2>/dev/null || true
if [ -n "${SOAK_RUNS:-}" ]; then
    kill "$sampler" 2>/dev/null || true
    wait "$sampler" 2>/dev/null || true
    sampler=''
fi
rm -rf "$dir/cache"
start "$@"
from=$(date +%s%N)
info >/dev/null
echo "a new server with an empty cache opened it in $((($(date +%s%N) - from) / 1000000)) ms, and stored again what the one that was killed had not flushed"
reading --workload read
reading --workload read
reading --workload scan
sent=$(metric durable_db_storage_sent_bytes_total)
written=$(metric 'durable_db_request_seconds_count{kind="commit"}')
bench --clients "$clients" --workload write --value-bytes "$value" --random-values --seconds $((seconds * 2)) | tee "$dir/write.out"
alive
# What was written is what was taken: those told to wait were answered too,
# and counted with them. A request refused by the server before it reached
# the database is not, and is not written: none is, with one database.
waited=$(sed -nE 's/.*told to wait ([0-9]+).*/\1/p' "$dir/write.out")
written=$((($(metric 'durable_db_request_seconds_count{kind="commit"}') - written - ${waited:-0}) * value))
sent=$(($(metric durable_db_storage_sent_bytes_total) - sent))
held=$(info)
echo "after that the database holds $(mib "$(echo "$held" | field bytes)") MiB in $(tables "$held"); $(mib "$sent") MiB were sent to storage, $(awk -v s="$sent" -v w="$written" 'BEGIN { printf "%.1f", s / (w > 0 ? w : 1) }') times what was written"

# The quota is set to a hundredth more than the database held when every
# key had been written once. It holds more than that now, because what was
# written over is still in its tables. The next write has no room: it is
# told to wait while everything is merged, and taken when that has made room.
quota=$((filled + filled / 100))
held=$(info | field bytes)
fetched=$(metric durable_db_storage_received_bytes_total)
curl -fsS -m 60 -o /dev/null -X PATCH -H "authorization: Bearer $token" \
    -H 'content-type: application/json' -d "{\"max_bytes\":$quota}" "$url/v1/databases/size-0"
from=$(date +%s%N)
waits=0
while :; do
    code=$(curl -sS -m 60 -o /dev/null -w '%{http_code}' -H "authorization: Bearer $token" \
        -H 'content-type: application/json' -d '{"writes":[{"key":"at-the-quota","value":"one more"}]}' \
        "$url/v1/databases/size-0/commit")
    [ "$code" = 503 ] && [ "$waits" -lt 36000 ] || break
    waits=$((waits + 1))
    sleep 0.1
done
took=$((($(date +%s%N) - from) / 1000000))
alive
[ "$code" = 200 ] || { echo "with its quota set to $(mib "$quota") MiB, under the $(mib "$held") MiB it held, a write was answered $code after $took ms"; exit 1; }
after=$(info)
echo "with its quota set to $(mib "$quota") MiB, under the $(mib "$held") MiB it held, a write was told to wait for $took ms while everything was merged, for which $(mib $(($(metric durable_db_storage_received_bytes_total) - fetched))) MiB were read from storage, and then taken; the database then held $(mib "$(echo "$after" | field bytes)") MiB in $(tables "$after")"
echo "most memory of the second server: $(peak) MiB"
failed=$((failed + $(failures)))

# A third server, after one that was told to stop and released the database.
kill "$server"
wait "$server" 2>/dev/null || true
rm -rf "$dir/cache"
start "$@"
from=$(date +%s%N)
info >/dev/null
echo "a third server with an empty cache, after one that was stopped and not killed, opened it in $((($(date +%s%N) - from) / 1000000)) ms"
from=$(date +%s%N)
"$bin" delete size-0 --url "$url" --key-file "$dir/admin.key" >/dev/null
echo "the database was deleted in $((($(date +%s%N) - from) / 1000000)) ms"
alive
echo "requests to storage that failed: $((failed + $(failures))); the servers said $(grep -c 'the store refuses' "$dir/server.log") times that the store refuses"
if [ -n "$link" ]; then
    kill -TERM "$link"
    wait "$link" 2>/dev/null || true
    link=''
    awk '/^carried/ { printf "the link carried %d MiB up and %d MiB down\n", $2 / 1048576, $6 / 1048576 }' "$dir/link"
fi
kill "$server"
wait "$server" 2>/dev/null || true
server=''
if grep -q 'panicked at' "$dir/server.log"; then
    grep -A1 'panicked at' "$dir/server.log" | head -6
    echo 'A SERVER PANICKED' >&2
    exit 1
fi
if grep -q 'has not ended' "$dir/server.log"; then
    grep 'has not ended' "$dir/server.log" | tail -1
    echo 'A ROUND OF HOUSEKEEPING DID NOT END IN TIME' >&2
    exit 1
fi
