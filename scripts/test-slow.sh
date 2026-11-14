#!/bin/sh
# Run every test alone on a fraction of one processor. A test that races a
# timer against its own work passes on a fast machine and fails on a slower
# one; this finds it before a build machine does (docs/verification.md,
# finding 26). Needs systemd-run. Takes several minutes.
#
#   scripts/test-slow.sh [percent of one processor, default 28] [suite, default all]
set -eu
percent=${1:-28}
suite=${2:-}
cd "$(dirname "$0")/.."
binaries=$(cargo test --no-run ${suite:+--test "$suite"} 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p')
[ -n "$binaries" ] || { echo 'no test binaries were built' >&2; exit 1; }
failed=0
for binary in $binaries; do
    for name in $("$binary" --list 2>/dev/null | sed -n 's/: test$//p'); do
        if ! out=$(timeout 900 systemd-run --user --scope -q -p CPUQuota="$percent%" -p MemoryMax=4G \
            "$binary" --exact "$name" --test-threads=1 2>&1); then
            failed=$((failed + 1))
            echo "FAILED at $percent%: $(basename "$binary" | sed 's/-[0-9a-f]*$//') $name"
            echo "$out" | grep -E 'panicked|never happened|left:|right:|assertion' | head -5
        fi
    done
done
[ "$failed" -eq 0 ] && echo "every test passes alone at $percent% of a processor"
exit "$((failed > 0))"
