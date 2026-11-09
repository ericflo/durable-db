#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tools="$repo/.tools"
mkdir -p "$tools"
jar="$tools/tla2tools-1.7.4.jar"
requested=${1:-all}
case "$requested" in *[!A-Za-z0-9_-]*|'') echo 'Invalid model name' >&2; exit 2 ;; esac
if [ ! -f "$jar" ]; then
  curl --proto '=https' --tlsv1.2 -fLsS https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar -o "$jar"
fi
expected=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
if command -v sha256sum >/dev/null; then actual=$(sha256sum "$jar" | cut -d ' ' -f1); else actual=$(shasum -a 256 "$jar" | cut -d ' ' -f1); fi
[ "$actual" = "$expected" ] || { echo 'TLC checksum mismatch' >&2; exit 1; }
[ "$requested" != prepare ] || exit 0
cd "$repo/spec"
check_model() {
  checks=$1
  [ -f "$checks" ] || { echo "Missing model descriptor: $checks" >&2; exit 1; }
  module=$(basename "$checks" .checks)
  # Each descriptor is independent, so adding/changing another model does not
  # invalidate this model's Docker cache. Every negative control is mandatory.
  while read -r kind config tag heap invariant; do
    case "$kind" in ''|\#*) continue ;; safe|unsafe) ;; *) echo 'Invalid check kind' >&2; exit 1 ;; esac
    log="$tools/model-$tag.log"
    if [ "$kind" = safe ]; then
      "${JAVA:-java}" -XX:+UseParallelGC "-Xmx$heap" -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-$tag" -config "$config" "$module.tla" > "$log" 2>&1
      tail -n 9 "$log"
    else
      set +e
      "${JAVA:-java}" -XX:+UseParallelGC "-Xmx$heap" -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-$tag" -config "$config" "$module.tla" > "$log" 2>&1
      status=$?
      set -e
      [ "$status" = 12 ] && grep -q "Invariant $invariant is violated" "$log" || { cat "$log"; echo 'Missing expected negative-control counterexample' >&2; exit 1; }
      echo "$module negative control: $config detects $invariant."
    fi
  done < "$checks"
}
if [ "$requested" = all ]; then
  for checks in "$repo"/spec/*.checks; do check_model "$checks"; done
else
  check_model "$repo/spec/$requested.checks"
fi
