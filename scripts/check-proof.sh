#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
case "${1:-PublicationSafety}" in
  PublicationSafety) module=PublicationSafety; tag=publication; negative=fence; action='Fence(c, TRUE)' ;;
  Journal) module=Journal; tag=journal; negative=journal-gap; action='Consume(n, TRUE)' ;;
  GrantBounds) module=GrantBounds; tag=grant-bounds ;;
  RootControl) module=RootControl; tag=root-control ;;
  *) echo 'Usage: scripts/check-proof.sh [PublicationSafety|Journal|GrantBounds|RootControl]' >&2; exit 2 ;;
esac
proof_dir=spec
case "$module" in GrantBounds|RootControl) proof_dir=spec/candidates ;; esac
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
  "$proof_dir/${module}Proof.tla" > ".tools/proof-$tag.log" 2>&1
grep -E 'All [1-9][0-9]* obligations proved' ".tools/proof-$tag.log"
tail -n 4 ".tools/proof-$tag.log"
# Recheck a changed protocol with fresh fingerprints. The intended induction
# step must fail; a parser or tool-launch error is not a passing control.
mkdir "$work/unsafe"
cp "$proof_dir/${module}Proof.tla" "$work/unsafe/"
check_negative() {
  name=$1
  required=$2
  if "$checker" --strict --nofp --threads 2 --cache-dir "$work/$name-cache" \
    "$work/unsafe/${module}Proof.tla" > ".tools/proof-negative-$name.log" 2>&1; then
    echo 'Unsafe proof unexpectedly succeeded' >&2; exit 1
  fi
  grep -Eq '[1-9][0-9]*/[1-9][0-9]* obligations failed' ".tools/proof-negative-$name.log"
  grep -Fq "$required" ".tools/proof-negative-$name.log"
  echo "TLAPS negative control: $name fails its required proof obligation."
}
case "$module" in
  PublicationSafety)
    sed 's/IF advance THEN fence + 1 ELSE fence/IF advance THEN fence ELSE fence/' \
      spec/PublicationSafety.tla > "$work/unsafe/PublicationSafety.tla"
    check_negative "$negative" "$action" ;;
  Journal)
    sed 's/n = writer\.durable + 1/TRUE/' spec/Journal.tla > "$work/unsafe/Journal.tla"
    check_negative "$negative" "$action" ;;
  GrantBounds)
    sed 's/2 \* lease + 3 \* error/2 * lease + error/' \
      spec/candidates/GrantBounds.tla > "$work/unsafe/GrantBounds.tla"
    check_negative grant-drain 'DrainWait(lease, error)'
    sed 's/(remaining - 3 \* error) \\div 2/remaining \\div 2/' \
      spec/candidates/GrantBounds.tla > "$work/unsafe/GrantBounds.tla"
    check_negative grant-child 'ChildDuration(remaining, error)' ;;
  RootControl)
    sed 's/IF UnsafeStaleRenewal/IF TRUE/' \
      spec/candidates/RootControl.tla > "$work/unsafe/RootControl.tla"
    check_negative root-renewal 'ApplyRenew(p)' ;;
esac
