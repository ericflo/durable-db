#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
case "${1:-PublicationSafety}" in
  PublicationSafety) module=PublicationSafety; tag=publication; negative=fence; action='Fence(c, TRUE)' ;;
  Journal) module=Journal; tag=journal; negative=journal-gap; action='Consume(n, TRUE)' ;;
  *) echo 'Usage: scripts/check-proof.sh [PublicationSafety|Journal]' >&2; exit 2 ;;
esac
if [ -z "${TLAPM:-}" ]; then
  scripts/install-tlaps.sh "$repo/.tools/proof-tools"
  checker="$repo/.tools/proof-tools/tlapm/bin/tlapm"
else
  checker=$TLAPM
fi
[ "$("$checker" --version)" = c8ddc44 ] || { echo 'Proof checker version mismatch' >&2; exit 1; }
mkdir -p .tools
work=$(mktemp -d "$repo/.tools/proof-check.XXXXXX")
trap 'rm -rf "$work"' EXIT
"$checker" --strict --nofp --threads 2 --timing --cache-dir "$work/safe-cache" \
  "spec/${module}Proof.tla" > ".tools/proof-$tag.log" 2>&1
grep -E 'All [1-9][0-9]* obligations proved' ".tools/proof-$tag.log"
tail -n 4 ".tools/proof-$tag.log"
# Recheck a changed protocol with fresh fingerprints. The intended induction
# step must fail; a parser or tool-launch error is not a passing control.
mkdir "$work/unsafe"
cp "spec/${module}Proof.tla" "$work/unsafe/"
case "$module" in
  PublicationSafety)
    sed 's/IF advance THEN fence + 1 ELSE fence/IF advance THEN fence ELSE fence/' \
      spec/PublicationSafety.tla > "$work/unsafe/PublicationSafety.tla" ;;
  Journal)
    sed 's/n = writer\.durable + 1/TRUE/' spec/Journal.tla > "$work/unsafe/Journal.tla" ;;
esac
if "$checker" --strict --nofp --threads 2 --cache-dir "$work/unsafe-cache" \
  "$work/unsafe/${module}Proof.tla" > ".tools/proof-negative-$negative.log" 2>&1; then
  echo 'Unsafe proof unexpectedly succeeded' >&2; exit 1
fi
grep -Eq '[1-9][0-9]*/[1-9][0-9]* obligations failed' ".tools/proof-negative-$negative.log"
grep -Fq "$action" ".tools/proof-negative-$negative.log"
echo "TLAPS negative control: $negative fails the required induction step."
