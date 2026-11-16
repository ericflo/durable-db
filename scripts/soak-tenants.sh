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
# minute here, and as long as its link takes to carry the database twice
# over on a bucket.
#
#   scripts/soak-tenants.sh
#   DURABLE_DB          the binary (default durable-db)
#   SOAK_LARGE          how many large databases (default 2)
#   SOAK_GIB            the size of each, in GiB (default 4)
#   SOAK_CLIENTS        clients that write the small database (default 64)
#   SOAK_READ_CLIENTS   clients that read it (default 8)
#   SOAK_DIR            where to put the data (default: a temporary directory)
#   SOAK_PORT           the server's port (default 17784)
#
# Needs four times the size of the large databases together in free disk.
set -eu
bin=$(command -v "${DURABLE_DB:-durable-db}")
large=${SOAK_LARGE:-2}
gib=${SOAK_GIB:-4}
clients=${SOAK_CLIENTS:-64}
readers=${SOAK_READ_CLIENTS:-8}
value=4096
keys=$((gib * 1073741824 / value))
url="http://127.0.0.1:${SOAK_PORT:-17784}"
dir=$(mktemp -d -p "${SOAK_DIR:-${TMPDIR:-/tmp}}" durable-db-tenants.XXXXXX)
server='' sampler='' quotas=''
cleanup() {
    for pid in $sampler $quotas $server; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -rf "$dir"
}
trap cleanup EXIT INT TERM

od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$dir/admin.key"
chmod 600 "$dir/admin.key"
mkdir "$dir/data"
cp "$dir/admin.key" "$dir/data/admin.key"
systemd-run --user --scope -q -p MemoryMax=1G -p MemorySwapMax=0 -p CPUQuota=400% \
    "$bin" serve --listen "${url#http://}" --data "$dir/data" \
    --cache-dir "$dir/cache" --cache-dir-bytes 1073741824 \
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
echo "the server runs $("$bin" --version) on a local directory; $large databases are filled with $keys keys of $value bytes each, $gib GiB, by 64 clients; $clients clients write a small one and $readers read it"
bench --database big --databases "$large" --keys "$keys" --clients 64 --workload fill --max-bytes $((gib * 4 * 1073741824))
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
kill "$server"
wait "$server" 2>/dev/null || true
server=''
if grep -q 'panicked at' "$dir/server.log"; then
    grep -A1 'panicked at' "$dir/server.log" | head -6
    echo 'A SERVER PANICKED' >&2
    exit 1
fi
