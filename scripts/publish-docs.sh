#!/bin/sh
set -eu
source_dir=$(CDPATH= cd -- "${1:-.site}" && pwd)
: "${DOCS_DEPLOY_KEY:?Woodpecker docs_deploy_key is required}"
: "${CI_COMMIT_SHA:?The verified source commit is required}"
case "$CI_COMMIT_SHA" in *[!0-9a-f]*|'') echo 'Invalid source commit' >&2; exit 2 ;; esac
[ "${#CI_COMMIT_SHA}" -eq 40 ] || { echo 'Invalid source commit length' >&2; exit 2; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
umask 077
printf '%s\n' "$DOCS_DEPLOY_KEY" > "$tmp/key"
unset DOCS_DEPLOY_KEY
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_NOSYSTEM=1
# GitHub's published Ed25519 host key is checked into the project.
cp scripts/github_known_hosts "$tmp/known_hosts"
export GIT_SSH_COMMAND="ssh -i $tmp/key -o BatchMode=yes -o ConnectTimeout=15 -o IdentitiesOnly=yes -o UserKnownHostsFile=$tmp/known_hosts -o StrictHostKeyChecking=yes"
git clone --depth 1 ssh://git@ssh.github.com:443/ericflo/durable-db-site.git "$tmp/site"
# Generated docs-only checkout: replace its tracked public files, never source or private plans.
git -C "$tmp/site" rm -r --ignore-unmatch . >/dev/null
cp -a "$source_dir/." "$tmp/site/"
# Pages can reject a deployment after its source push succeeds. This public
# marker binds delivery to the exact source that passed Woodpecker's checks.
printf '{"source_commit":"%s"}\n' "$CI_COMMIT_SHA" > "$tmp/site/deployment.json"
git -C "$tmp/site" add .
if ! git -C "$tmp/site" diff --cached --quiet; then
  git -C "$tmp/site" -c user.name=durable-db-ci -c user.email=durable-db-ci@users.noreply.github.com commit -m "Publish docs for $CI_COMMIT_SHA"
  git -C "$tmp/site" push origin main
fi
deadline=$(($(date +%s) + 600))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if curl --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 20 \
      --silent --fail -H 'Cache-Control: no-cache' \
      "https://ericflo.github.io/durable-db-site/deployment.json?source=$CI_COMMIT_SHA" \
      -o "$tmp/published.json" \
      && cmp -s "$tmp/site/deployment.json" "$tmp/published.json"; then
    echo "Public Pages deployment verified for $CI_COMMIT_SHA"
    exit 0
  fi
  echo 'Waiting for Pages to serve the verified source commit...'
  sleep 10
done
echo 'Site source was pushed, but public Pages delivery was not confirmed within 10 minutes.' >&2
exit 1
