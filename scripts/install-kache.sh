#!/bin/sh
set -eu
dest=${1:-.tools/bin}
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) target=x86_64-unknown-linux-musl; hash=6c59665faf1b14d0944391e6f2a1b510195e6be92c63060a9d1770ed00676824 ;;
  Linux-aarch64) target=aarch64-unknown-linux-musl; hash=8c5ca4d1d4b6fc66e5175c8a1150e1daa85099080646322058d9083c72986bba ;;
  Darwin-arm64) target=aarch64-apple-darwin; hash=b46139f7d42895c62962450886dd35f4554c17531209ea69b2db053d2a142bcb ;;
  Darwin-x86_64) target=x86_64-apple-darwin; hash=0ab8b29f659d159de78f70376c6ffa7730879f18fcde397567b683796eb5fd73 ;;
  *) echo 'Unsupported kache platform; install the pinned v2.0.0 client and engine manually.' >&2; exit 1 ;;
esac
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
curl --proto '=https' --tlsv1.2 -fLsS "https://github.com/kunobi-ninja/kache/releases/download/v2.0.0/kache-$target.tar.gz" -o "$tmp/kache.tar.gz"
if command -v sha256sum >/dev/null; then actual=$(sha256sum "$tmp/kache.tar.gz" | cut -d ' ' -f1); else actual=$(shasum -a 256 "$tmp/kache.tar.gz" | cut -d ' ' -f1); fi
[ "$actual" = "$hash" ] || { echo 'kache checksum mismatch' >&2; exit 1; }
mkdir -p "$dest"
tar xzf "$tmp/kache.tar.gz" -C "$dest" kache kache-engine
"$dest/kache" --version
