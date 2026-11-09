#!/bin/sh
# Verified upstream snapshot. The asset ID and hash pin bytes even though its tag rolls.
set -eu
case "$(uname -s)/$(uname -m)" in Linux/x86_64) ;; *) echo 'Pinned TLAPS bundle requires Linux x86_64; provide TLAPM built from c8ddc44.' >&2; exit 1 ;; esac
proof_dir=${1:?usage: install-tlaps.sh DESTINATION}
mkdir -p "$proof_dir"
archive="$proof_dir/toolchain.tar.gz"
expected=fa6d537fb62f8526e7dd0d716c88e3d18931cc5c6b5d9ba1949afdb8096fb758
if [ ! -f "$archive" ]; then
  trap 'rm -f "$archive.part"' EXIT
  if [ -n "${TLAPS_ARCHIVE:-}" ]; then
    cp "$TLAPS_ARCHIVE" "$archive.part"
  else
    curl --proto '=https' --tlsv1.2 --retry 3 --max-time 180 -fLsS \
      -H 'Accept: application/octet-stream' \
      https://api.github.com/repos/tlaplus/tlapm/releases/assets/721898594 -o "$archive.part"
  fi
  printf '%s  %s\n' "$expected" "$archive.part" | sha256sum -c -
  mv "$archive.part" "$archive"
fi
printf '%s  %s\n' "$expected" "$archive" | sha256sum -c -
tar -xzf "$archive" -C "$proof_dir"
[ "$("$proof_dir/tlapm/bin/tlapm" --version)" = c8ddc44 ] || { echo 'Unexpected TLAPS version' >&2; exit 1; }
