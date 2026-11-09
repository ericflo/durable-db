#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
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
  spec/PublicationSafetyProof.tla > .tools/proof-publication.log 2>&1
grep -Eq 'All [1-9][0-9]* obligations proved' .tools/proof-publication.log
tail -n 4 .tools/proof-publication.log
# Recheck a changed protocol with fresh fingerprints. A missing fence must make
# the induction step fail, not merely trigger a parser or tool-launch error.
mkdir "$work/unsafe"
cp spec/PublicationSafetyProof.tla "$work/unsafe/"
sed 's/IF advance THEN fence + 1 ELSE fence/IF advance THEN fence ELSE fence/' \
  spec/PublicationSafety.tla > "$work/unsafe/PublicationSafety.tla"
if "$checker" --strict --nofp --threads 2 --cache-dir "$work/unsafe-cache" \
  "$work/unsafe/PublicationSafetyProof.tla" > .tools/proof-negative-fence.log 2>&1; then
  echo 'Unsafe publication proof unexpectedly succeeded' >&2; exit 1
fi
grep -Eq '[1-9][0-9]*/[1-9][0-9]* obligations failed' .tools/proof-negative-fence.log
grep -q 'Fence(c, TRUE)' .tools/proof-negative-fence.log
echo 'TLAPS negative control: omitting the collector fence fails the inductive proof.'
