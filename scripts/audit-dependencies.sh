#!/bin/sh
# Run outside Docker build caching: new advisories must fail an unchanged lockfile.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
case "$(uname -s):$(uname -m)" in
  Linux:x86_64) ;;
  *) echo 'The pinned audit tool currently supports Linux x86_64.' >&2; exit 1 ;;
esac
report=${1:-.tools/dependency-audit.json}
mkdir -p "$(dirname -- "$report")"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
asset=cargo-audit-x86_64-unknown-linux-musl-v0.22.2
curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
  --connect-timeout 10 --max-time 120 \
  "https://github.com/rustsec/rustsec/releases/download/cargo-audit/v0.22.2/$asset.tgz" \
  -o "$tmp/audit.tgz"
echo "7fb9497f8594b389e5fce5ef9b92db08432996895b2e0c5a0167a69ed445c428  $tmp/audit.tgz" | sha256sum -c -
tar xzf "$tmp/audit.tgz" -C "$tmp" "$asset/cargo-audit"
status=0
CARGO_HOME="$tmp/cargo" "$tmp/$asset/cargo-audit" audit --deny warnings \
  --db "$tmp/advisory-db" --json > "$report" || status=$?
cat "$report"
exit "$status"
