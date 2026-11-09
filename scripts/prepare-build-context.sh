#!/bin/sh
# Build the committed tree with stable permissions, independent of PVC metadata.
set -eu
umask 022
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
commit=$(git -c safe.directory="$repo" -C "$repo" rev-parse --verify 'HEAD^{commit}')
[ "$commit" = "${CI_COMMIT_SHA:-$commit}" ] || {
  echo 'Build checkout does not match the pipeline commit' >&2
  exit 1
}
context=$(mktemp -d "${TMPDIR:-/tmp}/durable-db-build.XXXXXX")
archive="$context/source.tar"
success=no
trap 'if [ "$success" != yes ]; then rm -rf "$context"; fi' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
git -c safe.directory="$repo" -c tar.umask=0022 -C "$repo" \
  archive --format=tar --output="$archive" "$commit"
mkdir "$context/tree"
tar -xf "$archive" -C "$context/tree"
rm "$archive"
success=yes
printf '%s\n' "$context"
