#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tools="$repo/.tools"
mkdir -p "$tools"
jar="$tools/tla2tools-1.7.4.jar"
if [ ! -f "$jar" ]; then
  curl --proto '=https' --tlsv1.2 -fLsS https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar -o "$jar"
fi
expected=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
if command -v sha256sum >/dev/null; then actual=$(sha256sum "$jar" | cut -d ' ' -f1); else actual=$(shasum -a 256 "$jar" | cut -d ' ' -f1); fi
[ "$actual" = "$expected" ] || { echo 'TLC checksum mismatch' >&2; exit 1; }
cd "$repo/spec"
"${JAVA:-java}" -XX:+UseParallelGC -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-safe" -config Cluster.cfg Cluster.tla > "$tools/model-safe.log" 2>&1
tail -n 9 "$tools/model-safe.log"
set +e
"${JAVA:-java}" -XX:+UseParallelGC -Xmx1g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-unsafe" -config UnsafeGC.cfg Cluster.tla > "$tools/model-unsafe.log" 2>&1
status=$?
set -e
[ "$status" = 12 ] && grep -q 'Invariant ReferencedObjectsExist is violated' "$tools/model-unsafe.log" || { cat "$tools/model-unsafe.log"; echo 'Negative control did not produce the expected counterexample' >&2; exit 1; }
echo 'Negative control: missing collection fence produces the expected counterexample.'

"${JAVA:-java}" -XX:+UseParallelGC -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-bucket" -config Bucket.cfg Bucket.tla > "$tools/model-bucket.log" 2>&1
tail -n 9 "$tools/model-bucket.log"
set +e
"${JAVA:-java}" -XX:+UseParallelGC -Xmx1g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-bucket-unsafe" -config UnsafeBucket.cfg Bucket.tla > "$tools/model-bucket-unsafe.log" 2>&1
status=$?
set -e
[ "$status" = 12 ] && grep -q 'Invariant ReferencedManifestsExist is violated' "$tools/model-bucket-unsafe.log" || { cat "$tools/model-bucket-unsafe.log"; exit 1; }
echo 'B2 negative control: missing bucket fence deletes a published manifest.'

"${JAVA:-java}" -XX:+UseParallelGC -Xmx1g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-read" -config ReadBarrier.cfg ReadBarrier.tla > "$tools/model-read.log" 2>&1
tail -n 9 "$tools/model-read.log"
set +e
"${JAVA:-java}" -XX:+UseParallelGC -Xmx1g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-read-unsafe" -config UnsafeRead.cfg ReadBarrier.tla > "$tools/model-read-unsafe.log" 2>&1
status=$?
set -e
[ "$status" = 12 ] && grep -q 'Invariant ReadLinearizesAtValidation is violated' "$tools/model-read-unsafe.log" || { cat "$tools/model-read-unsafe.log"; exit 1; }
echo 'Read negative control: skipping CAS validation returns a stale snapshot.'

"${JAVA:-java}" -XX:+UseParallelGC -Xmx1g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-directory" -config Directory.cfg Directory.tla > "$tools/model-directory.log" 2>&1
tail -n 9 "$tools/model-directory.log"
set +e
"${JAVA:-java}" -XX:+UseParallelGC -Xmx1g -cp "$jar" tlc2.TLC -workers 2 -metadir "$tools/tlc-directory-unsafe" -config UnsafeDirectory.cfg Directory.tla > "$tools/model-directory-unsafe.log" 2>&1
status=$?
set -e
[ "$status" = 12 ] && grep -q 'Invariant WitnessReachability is violated' "$tools/model-directory-unsafe.log" || { cat "$tools/model-directory-unsafe.log"; exit 1; }
echo 'Directory negative control: a wrong witness can hide a reachable page.'
