#!/bin/sh
# Check a rolling upgrade: three servers of the old version on one directory
# are replaced by the new version one at a time while a ledger runs through
# all of them. Then one goes back to the old version and forward again, so
# that each version takes over databases the other wrote. The ledger must
# verify: no transfer lost, none applied twice.
#
#   scripts/test-upgrade.sh <old binary> <new binary>
#   UPGRADE_PORT   first of three ports (default 17741)
#   PAUSE          seconds between replacements (default 15)
set -eu
old=${1:?the old binary}
new=${2:?the new binary}
pause=${PAUSE:-15}
base=${UPGRADE_PORT:-17741}
dir=$(mktemp -d "${TMPDIR:-/tmp}/durable-db-upgrade.XXXXXX")
peers="127.0.0.1:$base,127.0.0.1:$((base + 1)),127.0.0.1:$((base + 2))"
urls="http://127.0.0.1:$base,http://127.0.0.1:$((base + 1)),http://127.0.0.1:$((base + 2))"
pid_0= pid_1= pid_2=
cleanup() {
    for pid in $pid_0 $pid_1 $pid_2; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -rf "$dir"
}
trap cleanup EXIT INT TERM

start() { # binary, server number
    port=$((base + $2))
    "$1" serve --listen "127.0.0.1:$port" --data "$dir/data" \
        --peers "$peers" --advertise "127.0.0.1:$port" >>"$dir/server-$2.log" 2>&1 &
    eval "pid_$2=$!"
    tries=0
    until curl -fs "http://127.0.0.1:$port/readyz" >/dev/null 2>&1; do
        tries=$((tries + 1))
        [ "$tries" -lt 100 ] || { echo "server $2 did not start"; cat "$dir/server-$2.log"; exit 1; }
        sleep 0.1
    done
    echo "server $2 runs $("$1" --version)"
}
replace() { # binary, server number: a graceful stop, which hands databases over
    eval "pid=\$pid_$2"
    kill "$pid"
    wait "$pid" 2>/dev/null || true
    start "$1" "$2"
}

start "$old" 0
start "$old" 1
start "$old" 2
timeout $((pause * 7 + 180)) "$new" bench --workload ledger --url "$urls" \
    --key-file "$dir/data/admin.key" --clients 16 --databases 8 \
    --seconds $((pause * 7)) >"$dir/ledger.out" 2>&1 &
ledger=$!
sleep "$pause"; replace "$new" 0
sleep "$pause"; replace "$new" 1
sleep "$pause"; replace "$new" 2
sleep "$pause"; replace "$old" 1
sleep "$pause"; replace "$new" 1
wait "$ledger" || true
tail -1 "$dir/ledger.out"
grep -q '^ledger verified' "$dir/ledger.out" || { echo 'THE LEDGER DID NOT VERIFY' >&2; exit 1; }
if grep -q 'panicked at' "$dir"/server-*.log; then
    grep -h -A1 'panicked at' "$dir"/server-*.log | head -6
    echo 'A SERVER PANICKED' >&2
    exit 1
fi
