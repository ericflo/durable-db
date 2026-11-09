#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
case "${1:-PublicationSafety}" in
  PublicationSafety) module=PublicationSafety; tag=publication; negative=fence; action='Fence(c, TRUE)' ;;
  Journal) module=Journal; tag=journal; negative=journal-gap; action='Consume(n, TRUE)' ;;
  GrantBounds) module=GrantBounds; tag=grant-bounds ;;
  RootControl) module=RootControl; tag=root-control ;;
  RootJournal) module=RootJournal; tag=root-journal ;;
  *) echo 'Usage: scripts/check-proof.sh [PublicationSafety|Journal|GrantBounds|RootControl|RootJournal]' >&2; exit 2 ;;
esac
proof_dir=spec
case "$module" in GrantBounds|RootControl|RootJournal) proof_dir=spec/candidates ;; esac
# Imported theorems are dependencies, not obligations checked by this module.
# CI may reuse the immutable proof-root-control stage; local runs check it first.
if [ "$module" = RootJournal ] && [ "${DURABLE_DB_ROOT_PROOF_CHECKED:-0}" != 1 ]; then
  "$repo/scripts/check-proof.sh" RootControl
fi
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
status=0
"$checker" --strict --nofp --threads 2 --timing --cache-dir "$work/safe-cache" \
  "$proof_dir/${module}Proof.tla" > ".tools/proof-$tag.log" 2>&1 || status=$?
if [ "$status" != 0 ]; then
  cat ".tools/proof-$tag.log" >&2
  exit "$status"
fi
grep -E 'All [1-9][0-9]* obligations proved' ".tools/proof-$tag.log"
tail -n 4 ".tools/proof-$tag.log"
# Recheck a changed protocol with fresh fingerprints. The intended induction
# step must fail; a parser or tool-launch error is not a passing control.
mkdir "$work/unsafe"
cp "$proof_dir/${module}Proof.tla" "$work/unsafe/"
if [ "$module" = RootJournal ]; then
  cp spec/candidates/RootControl.tla spec/candidates/RootControlProof.tla "$work/unsafe/"
fi
check_negative() {
  name=$1
  required=$2
  theorem=${3:-}
  set --
  if [ -n "$theorem" ]; then
    # Check the changed induction step directly; the complete positive proof
    # above still runs with fresh fingerprints. Do not repeat unrelated steps.
    range=$(awk -v theorem="$theorem" '$1 == "THEOREM" {
      if (first) { print first, NR - 1; exit }
      if ($2 == theorem) first = NR
    }' "$work/unsafe/${module}Proof.tla")
    first=${range% *}
    last=${range#* }
    [ "$first" -gt 0 ] && [ "$last" -ge "$first" ] || {
      echo 'Missing negative-control theorem range' >&2; exit 1;
    }
    set -- --toolbox "$first" "$last"
  fi
  if "$checker" --strict --nofp --threads 2 --cache-dir "$work/$name-cache" \
    "$@" "$work/unsafe/${module}Proof.tla" > ".tools/proof-negative-$name.log" 2>&1; then
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
  RootJournal)
    sed 's|(UnsafeJournalPermission \\/ RootValid(p))|TRUE|' \
      spec/candidates/RootJournal.tla > "$work/unsafe/RootJournal.tla"
    check_negative root-journal-permission 'AcknowledgeBatch(p)' AcknowledgeBatchPreserves ;;
esac
