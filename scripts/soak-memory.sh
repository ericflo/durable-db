#!/bin/sh
# Run one server with the flags and the memory limit of the Kubernetes
# manifest on a local directory, load it, and report the most memory it used.
# The server runs in a systemd scope limited to 1 GiB without swap and four
# CPUs: if it passes the limit it is killed, and this script fails. Last, the
# server is killed by this script in the middle of a load over 256 databases
# and started again, so that it opens them all at once while they are written
# to, and the most memory of that second server is reported as well.
#   DURABLE_DB     the binary (default durable-db)
#   SECONDS_EACH   seconds per load (default 60)
#   SOAK_DIR       where to put the data, about 4 GiB (default: a temporary directory)
set -eu
bin=${DURABLE_DB:-durable-db}
seconds=${SECONDS_EACH:-60}
dir=$(mktemp -d -p "${SOAK_DIR:-${TMPDIR:-/tmp}}" durable-db-soak.XXXXXX)
port=${SOAK_PORT:-17654}
url=http://127.0.0.1:$port
server=
stop() {
    [ -z "$server" ] || kill "$server" 2>/dev/null || true
    [ -z "$server" ] || wait "$server" 2>/dev/null || true
    rm -rf "$dir"
}
trap stop EXIT INT TERM

start() {
    systemd-run --user --scope -q -p MemoryMax=1G -p MemorySwapMax=0 -p CPUQuota=400% \
        "$bin" serve --listen "127.0.0.1:$port" --data "$dir/data" \
        --cache-dir "$dir/cache" --cache-dir-bytes 1073741824 \
        --cache-bytes 201326592 --memtable-bytes 100663296 \
        --body-bytes 134217728 --result-bytes 134217728 --max-connections 1024 \
        >>"$dir/server.log" 2>&1 &
    server=$!
    tries=0
    until curl -fs "$url/readyz" >/dev/null 2>&1; do
        tries=$((tries + 1))
        [ "$tries" -lt 100 ] || { echo "the server did not start"; cat "$dir/server.log"; exit 1; }
        sleep 0.1
    done
}
start

peak() { awk '/^VmHWM/ {printf "%d MiB", $2 / 1024}' "/proc/$server/status"; }
load() {
    name=$1
    shift
    printf '%-44s ' "$name"
    timeout $((seconds + 120)) "$bin" bench --url "$url" --key-file "$dir/data/admin.key" \
        --seconds "$seconds" "$@" 2>/dev/null
    kill -0 "$server" 2>/dev/null || { echo "the server was killed"; tail -5 "$dir/server.log"; exit 1; }
    printf '%-44s %s\n' "  most memory so far" "$(peak)"
}
# Values that do not compress, so that tables are as large as their data.
# Four databases of 160 MiB each, written over and over: merges of many
# tables run all the while.
load 'half commits of 16 KiB, 4 databases' \
    --workload mixed --clients 64 --databases 4 --value-bytes 16384 --keys 10000 --random-values
# Many databases at once: unflushed writes, remembered keys, open databases.
load 'commits of 1 KiB, 256 databases' \
    --workload write --clients 256 --databases 256 --value-bytes 1024 --random-values
# Reads of more data than the cache holds, from many connections.
load 'strong reads of 4 KiB, 512 clients' \
    --workload read --clients 512 --databases 8 --value-bytes 4096 --keys 20000 --random-values
first=$(peak)
# A server that is killed, as one that passed its limit would be, and the
# one that takes its place: every database is opened again at once, from
# what the first one stored, by the clients that were writing to it.
printf '%-44s ' 'the same, the server killed halfway'
timeout $((seconds + 120)) "$bin" bench --url "$url" --key-file "$dir/data/admin.key" \
    --seconds "$seconds" --workload write --clients 256 --databases 256 --value-bytes 1024 \
    --random-values >"$dir/killed.out" 2>/dev/null &
bench=$!
sleep $((seconds / 2))
kill -KILL "$server"
wait "$server" 2>/dev/null || true
start
wait "$bench" || true
tail -1 "$dir/killed.out"
kill -0 "$server" 2>/dev/null || { echo "the second server was killed"; tail -5 "$dir/server.log"; exit 1; }
printf '%-44s %s\n' "  most memory of the second server" "$(peak)"
if grep -q 'panicked at' "$dir/server.log"; then
    grep -A1 'panicked at' "$dir/server.log" | head -4
    echo 'THE SERVER PANICKED' >&2
    exit 1
fi
echo "most memory used: $first by the first server and $(peak) by the second, under a limit of 1024 MiB"
