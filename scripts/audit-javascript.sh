#!/bin/sh
# Audit uncached with verified Node/npm releases, without a full compiler image.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
case "$(uname -s):$(uname -m)" in
  Linux:x86_64) ;;
  *) echo 'The pinned audit tools currently support Linux x86_64.' >&2; exit 2 ;;
esac
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
fetch() {
  curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
    --connect-timeout 10 --max-time 120 "$1" -o "$2"
  printf '%s  %s\n' "$3" "$2" | sha256sum -c -
}
fetch https://nodejs.org/dist/v24.22.1/node-v24.22.1-linux-x64.tar.gz \
  "$tmp/node.tar.gz" eb6728e77c576a35162c122c45ccadf224d89ff00abc8dae46cc2897c6002d7c
fetch https://registry.npmjs.org/npm/-/npm-12.2.3.tgz \
  "$tmp/npm.tgz" fa0711dd513fa1386bbad3962a292cb813461cb57dedf07d6eb7efe76dffe897
tar -xzf "$tmp/node.tar.gz" -C "$tmp" node-v24.22.1-linux-x64/bin/node
tar -xzf "$tmp/npm.tgz" -C "$tmp"
PATH="$tmp/node-v24.22.1-linux-x64/bin:$PATH"
export PATH npm_config_cache="$tmp/cache"
node --version
node "$tmp/package/bin/npm-cli.js" --version
for directory in clients/javascript examples/web-app site; do
  node "$tmp/package/bin/npm-cli.js" --prefix "$directory" audit
done
