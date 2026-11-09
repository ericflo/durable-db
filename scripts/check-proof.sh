#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
module=${1:-PublicationSafety}
locate() {
  case "$1" in *[!A-Za-z0-9]*|'') echo 'Invalid proof module' >&2; return 2 ;; esac
  for directory in spec spec/candidates; do
    if [ -f "$directory/$1.proof" ]; then printf '%s\n' "$directory"; return; fi
  done
  echo "Missing proof descriptor: $1" >&2
  return 2
}
proof_dir=$(locate "$module")
descriptor="$proof_dir/$module.proof"
tag=$(awk -F '|' '$1 == "tag" { print $2 }' "$descriptor")
case "$tag" in *[!a-z0-9-]*|'') echo 'Invalid proof log tag' >&2; exit 2 ;; esac
# A descriptor is data. Adding a proof does not alter the runner or invalidate
# unrelated Docker proof stages. Every declared dependency and control is required.
controls=0
while IFS='|' read -r kind name theorem required mutation; do
  case "$kind" in
    ''|\#*) ;;
    tag) [ "$name" = "$tag" ] ;;
    dependency)
      locate "$name" >/dev/null
      # CI may inherit the exact checked dependency stage. Local runs check it.
      if [ "${DURABLE_DB_PROOF_DEPENDENCIES_CHECKED:-0}" != 1 ]; then
        "$repo/scripts/check-proof.sh" "$name" </dev/null
      fi ;;
    negative)
      case "$name" in *[!a-z0-9-]*|'') echo 'Invalid negative control name' >&2; exit 2 ;; esac
      [ -n "$required" ] && [ -n "$mutation" ]
      controls=$((controls + 1)) ;;
    *) echo 'Invalid proof descriptor entry' >&2; exit 2 ;;
  esac
done < "$descriptor"
[ "$controls" -gt 0 ] || { echo 'Proof requires a negative control' >&2; exit 2; }
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
trap 'exit 130' INT
trap 'exit 143' TERM
status=0
"$checker" --strict --nofp --threads 2 --timing --cache-dir "$work/safe-cache" \
  "$proof_dir/${module}Proof.tla" </dev/null > ".tools/proof-$tag.log" 2>&1 || status=$?
if [ "$status" != 0 ]; then
  cat ".tools/proof-$tag.log" >&2
  exit "$status"
fi
grep -E 'All [1-9][0-9]* obligations proved' ".tools/proof-$tag.log"
tail -n 4 ".tools/proof-$tag.log"
mkdir "$work/unsafe"
# Imported proof modules must be available to the mutated check as well.
# A subshell gives each recursion its own variables; copied files prevent cycles.
copy_dependencies() (
  dependency=$1
  [ ! -f "$work/unsafe/${dependency}Proof.tla" ] || exit 0
  directory=$(locate "$dependency")
  cp "$directory/$dependency.tla" "$directory/${dependency}Proof.tla" "$work/unsafe/"
  while IFS='|' read -r kind name rest; do
    [ "$kind" != dependency ] || copy_dependencies "$name"
  done < "$directory/$dependency.proof"
)
copy_dependencies "$module"
check_negative() {
  name=$1
  required=$2
  theorem=$3
  set --
  if [ -n "$theorem" ]; then
    # The complete positive proof still runs fresh. Focus mutations on the
    # changed induction theorem, without rechecking unrelated obligations.
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
  # Toolbox mode reads cancellation input; it must not consume descriptor rows.
  if "$checker" --strict --nofp --threads 2 --cache-dir "$work/$name-cache" \
    "$@" "$work/unsafe/${module}Proof.tla" </dev/null > ".tools/proof-negative-$name.log" 2>&1; then
    cat ".tools/proof-negative-$name.log" >&2
    echo 'Unsafe proof unexpectedly succeeded' >&2; exit 1
  fi
  if ! grep -Eq '[1-9][0-9]*/[1-9][0-9]* obligations failed' ".tools/proof-negative-$name.log" \
    || ! grep -Fq "$required" ".tools/proof-negative-$name.log"; then
    cat ".tools/proof-negative-$name.log" >&2
    echo "Missing expected proof failure: $name" >&2
    exit 1
  fi
  echo "TLAPS negative control: $name fails its required proof obligation."
}
completed=0
while IFS='|' read -r kind name theorem required mutation; do
  [ "$kind" = negative ] || continue
  sed "$mutation" "$proof_dir/$module.tla" > "$work/unsafe/$module.tla"
  if cmp -s "$proof_dir/$module.tla" "$work/unsafe/$module.tla"; then
    echo "Negative control did not change its model: $name" >&2
    exit 1
  fi
  check_negative "$name" "$required" "$theorem"
  completed=$((completed + 1))
done < "$descriptor"
[ "$completed" -eq "$controls" ] || { echo 'Incomplete negative controls' >&2; exit 1; }
