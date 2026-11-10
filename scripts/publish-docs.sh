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
inspect_after=$(($(date +%s) + 60))
retried=false
pages_failed() {
  # Public, read-only API calls: no personal or repository API token in CI.
  # Inspect at most once per minute, and only when the marker is still stale.
  python3 - "$1" <<'PY'
import json, sys, urllib.error, urllib.request
base = 'https://api.github.com/repos/ericflo/durable-db-site/'
def get(path):
    request = urllib.request.Request(base + path, headers={
        'Accept': 'application/vnd.github+json', 'User-Agent': 'durable-db-docs'})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read(65536))
try:
    deployments = get('deployments?environment=github-pages&per_page=1&sha=' + sys.argv[1])
    statuses = get('deployments/' + str(int(deployments[0]['id'])) + '/statuses?per_page=1') if deployments else []
    failed = bool(statuses and statuses[0].get('state') in ('failure', 'error'))
except (OSError, ValueError, KeyError, IndexError, TypeError):
    failed = False  # Rate limits, unavailable API or unknown state cannot authorize a retry.
sys.exit(0 if failed else 1)
PY
}
while [ "$(date +%s)" -lt "$deadline" ]; do
  if curl --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 20 \
      --silent --fail -H 'Cache-Control: no-cache' \
      "https://ericflo.github.io/durable-db-site/deployment.json?source=$CI_COMMIT_SHA" \
      -o "$tmp/published.json" \
      && cmp -s "$tmp/site/deployment.json" "$tmp/published.json"; then
    echo "Public Pages deployment verified for $CI_COMMIT_SHA"
    exit 0
  fi
  if [ "$retried" = false ] && [ "$(date +%s)" -ge "$inspect_after" ]; then
    inspect_after=$(($(date +%s) + 60))
    head=$(git -C "$tmp/site" rev-parse HEAD)
    if pages_failed "$head"; then
      # Retry the exact tree once, without overwriting another publisher. An
      # empty commit also avoids rebuilding or bypassing the checked artifact.
      git -C "$tmp/site" fetch origin main
      [ "$(git -C "$tmp/site" rev-parse origin/main)" = "$head" ] || {
        echo 'Pages source changed during publication; refusing to replace it.' >&2
        exit 1
      }
      git -C "$tmp/site" -c user.name=durable-db-ci -c user.email=durable-db-ci@users.noreply.github.com \
        commit --allow-empty -m "Retry Pages delivery for $CI_COMMIT_SHA"
      git -C "$tmp/site" push origin main
      retried=true
      echo 'Retrying failed Pages deployment once with the identical site tree.'
    fi
  fi
  echo 'Waiting for Pages to serve the verified source commit...'
  sleep 10
done
echo 'Site source was pushed, but public Pages delivery was not confirmed within 10 minutes.' >&2
exit 1
