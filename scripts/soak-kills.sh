#!/bin/sh
# A ledger through three servers on one directory while, again and again,
# one of them is stopped and started, each in its turn: killed outright three
# times in four, told to stop the fourth. Each server runs in a systemd scope
# limited to 1 GiB without swap and two CPUs, and is sampled before it is
# stopped. Printed: how many times a server was stopped, the most memory and
# the most open files any server had, and the ledger's result. The ledger
# must verify: no transfer lost, none applied twice. No server may panic.
#
#   scripts/soak-kills.sh
#   DURABLE_DB       the binary (default durable-db)
#   SOAK_SECONDS     how long the ledger runs (default 600)
#   PAUSE            seconds between stops (default 10)
#   SOAK_DIR         where to put the data (default: a temporary directory)
#   SOAK_PORT        first of three ports (default 17791)
set -eu
bin=${DURABLE_DB:-durable-db}
seconds=${SOAK_SECONDS:-600}
pause=${PAUSE:-10}
base=${SOAK_PORT:-17791}
dir=$(mktemp -d -p "${SOAK_DIR:-${TMPDIR:-/tmp}}" durable-db-kills.XXXXXX)
peers="127.0.0.1:$base,127.0.0.1:$((base + 1)),127.0.0.1:$((base + 2))"
urls="http://127.0.0.1:$base,http://127.0.0.1:$((base + 1)),http://127.0.0.1:$((base + 2))"
pid_0='' pid_1='' pid_2=''
cleanup() {
    for pid in $pid_0 $pid_1 $pid_2; do kill -KILL "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -rf "$dir"
}
trap cleanup EXIT INT TERM

start() { # server number
    port=$((base + $1))
    systemd-run --user --scope -q -p MemoryMax=1G -p MemorySwapMax=0 -p CPUQuota=200% \
        "$bin" serve --listen "127.0.0.1:$port" --data "$dir/data" \
        --peers "$peers" --advertise "127.0.0.1:$port" >>"$dir/server-$1.log" 2>&1 &
    eval "pid_$1=$!"
    tries=0
    until curl -fs "http://127.0.0.1:$port/readyz" >/dev/null 2>&1; do
        tries=$((tries + 1))
        [ "$tries" -lt 100 ] || { echo "server $1 did not start"; tail -5 "$dir/server-$1.log"; exit 1; }
        sleep 0.1
    done
}
most_memory=0 most_files=0
sample() { # pid: its most memory so far and its open files now
    memory=$(awk '/^VmHWM/ { print int($2 / 1024) }' "/proc/$1/status" 2>/dev/null || echo 0)
    files=$(ls "/proc/$1/fd" 2>/dev/null | wc -l)
    [ "${memory:-0}" -le "$most_memory" ] || most_memory=$memory
    [ "$files" -le "$most_files" ] || most_files=$files
}

start 0
start 1
start 2
timeout $((seconds + 240)) "$bin" bench --workload ledger --url "$urls" \
    --key-file "$dir/data/admin.key" --clients 16 --databases 8 \
    --seconds "$seconds" >"$dir/ledger.out" 2>&1 &
ledger=$!
stops=0
ends=$(($(date +%s) + seconds - pause))
while [ "$(date +%s)" -lt "$ends" ]; do
    sleep "$pause"
    n=$((stops % 3))
    eval "pid=\$pid_$n"
    kill -0 "$pid" 2>/dev/null || { echo "server $n was gone before it was stopped"; tail -5 "$dir/server-$n.log"; exit 1; }
    sample "$pid"
    if [ $((stops % 4)) -eq 3 ]; then kill -TERM "$pid"; else kill -KILL "$pid"; fi
    wait "$pid" 2>/dev/null || true
    start "$n"
    stops=$((stops + 1))
done
wait "$ledger" || true
for n in 0 1 2; do eval "sample \$pid_$n"; done
echo "a server was stopped $stops times in $seconds s; the most memory of any was $most_memory MiB, the most open files $most_files"
tail -1 "$dir/ledger.out"
grep -q '^ledger verified' "$dir/ledger.out" || { echo 'THE LEDGER DID NOT VERIFY' >&2; exit 1; }
if grep -q 'panicked at' "$dir"/server-*.log; then
    grep -h -A1 'panicked at' "$dir"/server-*.log | head -6
    echo 'A SERVER PANICKED' >&2
    exit 1
fi
