#!/bin/sh
# Large databases merged in full at once, and a small one written and read
# meanwhile, on one server.
#
# A server merges the tables of two databases at a time. A merge of a large
# database takes as long as reading it and writing it again, and a database
# that waits for its turn to merge gains a run with every flush, which every
# read of it looks in. Here SOAK_LARGE databases are filled with SOAK_GIB
# each, every key once with values that do not compress, and written over
# at keys taken at random for half a minute, so that each holds values
# written over. Then the quota of each is set to a hundredth above what it
# held when it was filled, and a write is made to each, which waits while
# everything in it is merged; they begin together. From then until every
# such write is taken, and for one round after, a small database is
# written by SOAK_CLIENTS clients and read at keys taken at random by
# SOAK_READ_CLIENTS, in rounds of five seconds. It is flushed for every
# 8 MiB written to it, about once a second with 64 clients.
#
# Printed: for every round, the writes and the reads of the small database
# answered in a second and the most runs it had, sampled every half second;
# how long each write at a quota waited; and what the server read from
# storage and sent to it from the moment the quotas were set until the last
# of those writes was taken, for all its databases together.
#
# One server with the flags of the Kubernetes manifest, in a systemd scope
# limited to 1 GiB without swap and four CPUs, on a local directory, which
# answers in a fraction of a millisecond: a merge of 4 GiB takes half a
# minute there, and as long as its link takes to carry the database twice
# over on a bucket. With SOAK_STORE=minio it is a community build of MinIO
# in a container held to 4 GiB and four CPUs, and with SOAK_LINK a slow link
# to it, as in scripts/soak-size.sh.
#
#   scripts/soak-tenants.sh
#   DURABLE_DB          the binary (default durable-db)
#   SOAK_LARGE          how many large databases (default 2)
#   SOAK_GIB            the size of each, in GiB (default 4)
#   SOAK_CLIENTS        clients that write the small database (default 64)
#   SOAK_READ_CLIENTS   clients that read it (default 8)
#   SOAK_MIB            the size of each large database in MiB, in the place of SOAK_GIB
#   SOAK_STORE          minio (default: a local directory)
#   SOAK_LINK           with minio, a slow link to it: UP,DOWN,RTT, bytes a second each way and a
#                       round trip in ms (scripts/slow-link.py; default: none)
#   SOAK_CACHE_MIB      the server's disk cache, in MiB (default 1024, the Kubernetes manifest's)
#   SOAK_LOG            a file to keep the server's log in; with RUST_LOG=durable_db=debug it
#                       has a line for each merge published (default: not kept)
#   STORE_PORT          the port MinIO is given (default 19320), and the link the next one
#   SOAK_DIR            where to put the data (default: a temporary directory)
#   SOAK_PORT           the server's port (default 17784)
#
# Needs four times the size of the large databases together in free disk.
set -eu
bin=$(command -v "${DURABLE_DB:-durable-db}")
large=${SOAK_LARGE:-2}
mib=${SOAK_MIB:-$((${SOAK_GIB:-4} * 1024))}
clients=${SOAK_CLIENTS:-64}
readers=${SOAK_READ_CLIENTS:-8}
value=4096
keys=$((mib * 1048576 / value))
url="http://127.0.0.1:${SOAK_PORT:-17784}"
dir=$(mktemp -d -p "${SOAK_DIR:-${TMPDIR:-/tmp}}" durable-db-tenants.XXXXXX)
server='' sampler='' quotas='' link=''
endpoint="http://127.0.0.1:${STORE_PORT:-19320}"
container=durable-db-tenants-$$
cleanup() {
    for pid in $sampler $quotas $server $link; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    [ "${SOAK_STORE:-}" != minio ] || docker rm -f -v "$container" >/dev/null 2>&1 || true
    [ -z "${SOAK_LOG:-}" ] || cp "$dir/server.log" "$SOAK_LOG" 2>/dev/null || true
    rm -rf "$dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

random() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
random 32 >"$dir/admin.key"
chmod 600 "$dir/admin.key"
where='a local directory'
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
        python3 "$(dirname "$0")/slow-link.py" $((${STORE_PORT:-19320} + 1)) "${endpoint#http://}" \
            "$up" "$down" "$rtt" >"$dir/link" 2>&1 &
        link=$!
        until grep -q ready "$dir/link" 2>/dev/null; do
            kill -0 "$link" 2>/dev/null || { cat "$dir/link"; exit 1; }
            sleep 0.1
        done
        served="http://127.0.0.1:$((${STORE_PORT:-19320} + 1))"
        where="MinIO over a link of $up bytes a second up and $down down, $rtt ms a round trip"
    fi
    export DURABLE_DB_BUCKET=plain DURABLE_DB_ENDPOINT="$served" DURABLE_DB_REGION=us-east-1 \
        DURABLE_DB_PREFIX="soak-$(random 4)" AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret"
    set -- --key-file "$dir/admin.key"
else
    mkdir "$dir/data"
    cp "$dir/admin.key" "$dir/data/admin.key"
    set -- --data "$dir/data"
fi
systemd-run --user --scope -q -p MemoryMax=1G -p MemorySwapMax=0 -p CPUQuota=400% \
    "$bin" serve --listen "${url#http://}" "$@" \
    --cache-dir "$dir/cache" --cache-dir-bytes $((${SOAK_CACHE_MIB:-1024} * 1048576)) \
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
alive() {
    kill -0 "$server" 2>/dev/null || { echo "the server was killed"; tail -5 "$dir/server.log"; exit 1; }
}
bench() {
    systemd-run --user --scope -q -p MemoryMax=2G -p CPUQuota=400% \
        timeout 86400 "$bin" bench --url "$url" --key-file "$dir/admin.key" --value-bytes "$value" \
        --random-values "$@" >/dev/null 2>&1
}
token=$("$bin" token --admin --key-file "$dir/admin.key")
info() { curl -fsS -m 60 -H "authorization: Bearer $token" "$url/v1/databases/$1"; }
field() { sed -nE "s/.*\"$1\":([0-9]+).*/\1/p"; }
json() { sed -nE "s/.*\"$1\": *([0-9.]+).*/\1/p" "$2" | head -1; }
metric() { curl -fs "$url/metrics" | awk -v name="$1" '$1 == name { print int($2) }'; }
mib() { echo $(($1 / 1048576)); }

names=$(seq -f 'big-%g' 0 $((large - 1)))
echo "the server runs $("$bin" --version) on $where; $large databases are filled with $keys keys of $value bytes each, $mib MiB, by 64 clients; $clients clients write a small one and $readers read it; a server's disk cache is ${SOAK_CACHE_MIB:-1024} MiB"
bench --database big --databases "$large" --keys "$keys" --clients 64 --workload fill --max-bytes $((mib * 4 * 1048576))
alive
for name in $names; do
    info "$name" | field bytes >"$dir/$name.filled"
done
bench --database big --databases "$large" --keys "$keys" --clients 64 --workload write --seconds 30
alive
bench --database small --keys 100000 --clients "$clients" --workload write --seconds 5
alive
for name in $names; do
    held=$(info "$name")
    echo "filled and written over for 30 s, $name holds $(mib "$(echo "$held" | field bytes)") MiB in $(echo "$held" | field runs) runs"
done
c=$(info small-0)
echo "the small one, written for 5 s, holds $(mib "$(echo "$c" | field bytes)") MiB in $(echo "$c" | field runs) runs"

# The runs of the small database, every half second.
while sleep 0.5; do
    echo "$(info small-0 2>/dev/null | field runs)"
done >"$dir/runs" &
sampler=$!

# Each large database is given a quota under what it holds, and a write;
# it is told to wait while everything is merged. Recorded when it is taken:
# how long it waited, and what the server had read from storage and sent
# to it by then.
at_the_quota() {
    filled=$(cat "$dir/$1.filled")
    curl -fsS -m 60 -o /dev/null -X PATCH -H "authorization: Bearer $token" \
        -H 'content-type: application/json' -d "{\"max_bytes\":$((filled + filled / 100))}" "$url/v1/databases/$1"
    from=$(date +%s%N)
    waits=0
    while :; do
        code=$(curl -sS -m 60 -o /dev/null -w '%{http_code}' -H "authorization: Bearer $token" \
            -H 'content-type: application/json' -d '{"writes":[{"key":"at-the-quota","value":"one more"}]}' \
            "$url/v1/databases/$1/commit")
        [ "$code" = 503 ] && [ "$waits" -lt 36000 ] || break
        waits=$((waits + 1))
        sleep 0.1
    done
    took=$((($(date +%s%N) - from) / 1000000))
    echo "$code $took $(metric durable_db_storage_received_bytes_total) $(metric durable_db_storage_sent_bytes_total)" >"$dir/$1.part"
    mv "$dir/$1.part" "$dir/$1.taken"
}
fetched=$(metric durable_db_storage_received_bytes_total)
sent=$(metric durable_db_storage_sent_bytes_total)
for name in $names; do
    at_the_quota "$name" &
    quotas="$quotas $!"
done
from=$(date +%s)
last=0 after=''
while [ -z "$after" ]; do
    after=yes
    for name in $names; do
        [ -e "$dir/$name.taken" ] || after=''
    done
    begun=$(($(date +%s) - from))
    bench --database small --keys 100000 --clients "$clients" --workload write --seconds 5 --output "$dir/write.json" &
    writer=$!
    bench --database small --keys 100000 --clients "$readers" --workload read --seconds 5 --output "$dir/read.json"
    wait "$writer"
    alive
    seen=$(wc -l <"$dir/runs")
    most=$(sed -n "$((last + 1)),${seen}p" "$dir/runs" | grep . | sort -n | tail -1)
    last=$seen
    echo "from ${begun} s${after:+, after every merge}: $(json requests_per_second "$dir/write.json") writes a second, $(json refused "$dir/write.json") told to wait; $(json requests_per_second "$dir/read.json") reads a second; ${most:-?} runs at most"
done
longest=0
for name in $names; do
    read -r code took received sent_by <"$dir/$name.taken"
    [ "$code" = 200 ] || { echo "$name: the write at the quota was answered $code after $took ms"; exit 1; }
    held=$(info "$name")
    echo "$name: the write at the quota waited $took ms; then $(mib "$(echo "$held" | field bytes)") MiB in $(echo "$held" | field runs) runs"
    if [ "$took" -gt "$longest" ]; then
        longest=$took merged_fetched=$((received - fetched)) merged_sent=$((sent_by - sent))
    fi
done
echo "until the last was taken, $(mib "$merged_fetched") MiB were read from storage and $(mib "$merged_sent") MiB sent to it"
echo "the most runs the small database had: $(grep . "$dir/runs" | sort -n | tail -1)"
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
