#!/bin/sh
# Keep research checks separate from the implemented protocol and its CI caches.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
candidate=${1:-}
case "$candidate" in
  WalHandoff|OwnedWal|DelegatedGrant) ;;
  *) echo 'Usage: scripts/check-candidate.sh WalHandoff|OwnedWal|DelegatedGrant' >&2; exit 2 ;;
esac

"$repo/scripts/check-model.sh" prepare
scratch=$(mktemp -d "${TMPDIR:-/tmp}/durable-db-candidate.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$scratch/scripts" "$scratch/spec" "$scratch/.tools"
cp "$repo/scripts/check-model.sh" "$scratch/scripts/"
cp "$repo/spec/candidates/$candidate.tla" "$repo/spec/candidates/$candidate.checks" "$scratch/spec/"
ln -s "$repo/.tools/tla2tools-1.7.4.jar" "$scratch/.tools/tla2tools-1.7.4.jar"
while read -r kind config rest; do
  case "$kind" in ''|\#*) continue ;; esac
  case "$config" in
    *[!A-Za-z0-9_.-]*|.*|'') echo 'Invalid candidate configuration' >&2; exit 2 ;;
  esac
  cp "$repo/spec/candidates/$config" "$scratch/spec/"
done < "$repo/spec/candidates/$candidate.checks"

status=0
"$scratch/scripts/check-model.sh" "$candidate" || status=$?
logs="$repo/.tools/candidates/$candidate"
mkdir -p "$logs"
for log in "$scratch"/.tools/model-*.log; do
  [ ! -f "$log" ] || cp "$log" "$logs/"
done
echo "Candidate logs: $logs"
exit "$status"
