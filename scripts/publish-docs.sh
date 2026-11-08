#!/bin/sh
set -eu
source_dir=$(CDPATH= cd -- "${1:-.site}" && pwd)
: "${DOCS_DEPLOY_KEY:?Woodpecker docs_deploy_key is required}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
umask 077
printf '%s\n' "$DOCS_DEPLOY_KEY" > "$tmp/key"
unset DOCS_DEPLOY_KEY
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_NOSYSTEM=1
# GitHub's published Ed25519 host key is checked into the project.
cp scripts/github_known_hosts "$tmp/known_hosts"
export GIT_SSH_COMMAND="ssh -i $tmp/key -o IdentitiesOnly=yes -o UserKnownHostsFile=$tmp/known_hosts -o StrictHostKeyChecking=yes"
git clone --depth 1 git@github.com:ericflo/durable-db-site.git "$tmp/site"
# Generated docs-only checkout: replace its tracked public files, never source or private plans.
git -C "$tmp/site" rm -r --ignore-unmatch . >/dev/null
cp -a "$source_dir/." "$tmp/site/"
git -C "$tmp/site" add .
if git -C "$tmp/site" diff --cached --quiet; then exit 0; fi
git -C "$tmp/site" -c user.name=durable-db-ci -c user.email=durable-db-ci@users.noreply.github.com commit -m "Publish docs for ${CI_COMMIT_SHA:-local}"
git -C "$tmp/site" push origin main
