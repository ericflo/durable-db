#!/bin/sh
# Model-check the protocol and require every negative control to fail.
# Usage: scripts/check-model.sh [config-name]   (default: all)
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
jar="$repo/.tools/tla2tools-1.7.4.jar"
mkdir -p "$repo/.tools"
if [ ! -f "$jar" ]; then
  curl --proto '=https' --tlsv1.2 -fLsS https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar -o "$jar"
fi
echo "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  $jar" | sha256sum -c - >/dev/null
# TLC sizes itself from the machine by default. Keep it small and bounded:
# a fixed heap, a fixed fingerprint set, a few workers, and a time limit.
heap=${TLC_HEAP:-2g}
workers=${TLC_WORKERS:-4}
limit=${TLC_TIMEOUT:-600}
jvm="-Xmx$heap -XX:MaxDirectMemorySize=$heap -XX:+UseParallelGC"
if command -v java >/dev/null 2>&1; then
  tlc() { (cd "$repo/spec" && timeout "$limit" java $jvm -cp "$jar" tlc2.TLC "$@"); }
else
  tlc() { timeout "$limit" docker run --rm --memory 6g --memory-swap 6g --cpus "$workers" \
    -u "$(id -u):$(id -g)" -v "$repo:/w" -w /w/spec eclipse-temurin:21-jre-alpine \
    java $jvm -cp /w/.tools/tla2tools-1.7.4.jar tlc2.TLC "$@"; }
fi
run() {
  name=$1; log="$repo/.tools/model-$name.log"
  set +e
  tlc -deadlock -workers "$workers" -metadir "../.tools/tlc-$name" -config "$name.cfg" DurableDb.tla > "$log" 2>&1
  status=$?
  set -e
  case "$name" in
    Unsafe*)
      if [ "$status" = 12 ] && grep -q 'Invariant .* is violated' "$log"; then
        echo "ok   $name: refuted ($(grep -o 'Invariant [A-Za-z]* is violated' "$log" | head -1))"
      else tail -20 "$log"; echo "FAIL $name: expected a counterexample" >&2; exit 1; fi ;;
    *)
      if [ "$status" = 0 ]; then echo "ok   $name: $(grep -E 'distinct states found' "$log" | tail -1)"
      else tail -40 "$log"; echo "FAIL $name" >&2; exit 1; fi ;;
  esac
}
if [ $# -gt 0 ]; then run "$1"; else
  for cfg in "$repo"/spec/*.cfg; do run "$(basename "$cfg" .cfg)"; done
fi
