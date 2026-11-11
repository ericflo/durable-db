import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

// Exercise the actual shell script with isolated transport/clock doubles.
// No credentials, network, repository mutation or real waiting are involved.
const commit = 'a'.repeat(40);
for (const [name, mode, source, status, retries] of [
  ['confirms the public source marker', 'match', commit, 0, 0],
  ['rejects stale Pages after a successful push', 'stale', commit, 1, 0],
  ['rejects an HTTP failure even with a matching body', 'error', commit, 1, 0],
  ['rejects an invalid source identity before publishing', 'match', 'invalid', 2, 0],
  ['retries an explicitly failed deployment once', 'retry', commit, 0, 1],
  ['does not retry pending or unknown deployment state', 'pending', commit, 1, 0],
  ['refuses to overwrite a newer publication', 'changed', commit, 1, 0],
  ['bounds repeated deployment failure to one retry', 'retry-fails', commit, 1, 1],
]) {
  test(name, () => {
    const scratch = mkdtempSync(join(tmpdir(), 'durable-db-publish-test-'));
    try {
      const bin = join(scratch, 'bin');
      const site = join(scratch, 'site');
      mkdirSync(bin);
      mkdirSync(site);
      writeFileSync(join(site, 'index.html'), '<p>Checked documentation</p>');
      const clock = join(scratch, 'clock');
      const retried = join(scratch, 'retried');
      writeFileSync(clock, '0\n');
      const shims = {
        git: `
if [ "$1" = clone ]; then
  for destination do :; done
  mkdir -p "$destination"
fi
if [ "$1" = -C ]; then shift 2; fi
if [ "$1" = diff ]; then exit 1; fi
if [ "$1" = rev-parse ]; then
  if [ "$2" = origin/main ] && [ "$MOCK_MODE" = changed ]; then
    echo another-publisher
  else
    echo current-head
  fi
fi
case " $* " in *" --allow-empty "*) echo retry >> "$MOCK_RETRIED" ;; esac
exit 0`,
        curl: `
for output do :; done
if [ "$MOCK_MODE" = match ] || [ "$MOCK_MODE" = error ] ||
   { [ "$MOCK_MODE" = retry ] && [ -f "$MOCK_RETRIED" ]; }; then
  printf '{"source_commit":"%s"}\\n' "$CI_COMMIT_SHA" > "$output"
else
  printf '{"source_commit":"stale"}\\n' > "$output"
fi
if [ "$MOCK_MODE" = error ]; then exit 22; fi`,
        date: 'cat "$MOCK_CLOCK"',
        sleep: 'now=$(cat "$MOCK_CLOCK"); echo "$((now + 60))" > "$MOCK_CLOCK"',
        python3: `
if [ "$1" = scripts/merge-docs.py ]; then
  printf '{"source_commit":"%s"}\\n' "$CI_COMMIT_SHA" > "$3/deployment.json"
  exit 0
fi
case "$MOCK_MODE" in retry|retry-fails|changed) exit 0 ;; *) exit 1 ;; esac`,
      };
      for (const [command, body] of Object.entries(shims)) {
        writeFileSync(join(bin, command), `#!/bin/sh\nset -eu\n${body}\n`, { mode: 0o755 });
      }
      const result = spawnSync('sh', ['scripts/publish-docs.sh', site], {
        encoding: 'utf8',
        timeout: 5_000,
        env: {
          ...process.env,
          PATH: `${bin}:${process.env.PATH}`,
          DOCS_DEPLOY_KEY: 'fixture-key',
          CI_COMMIT_SHA: source,
          MOCK_MODE: mode,
          MOCK_CLOCK: clock,
          MOCK_RETRIED: retried,
        },
      });
      assert.ifError(result.error);
      assert.equal(result.status, status, result.stderr);
      assert.equal(result.stdout.includes('Public Pages deployment verified'), status === 0);
      assert.equal(existsSync(retried) ? readFileSync(retried, 'utf8').trim().split('\n').length : 0, retries);
    } finally {
      rmSync(scratch, { recursive: true, force: true });
    }
  });
}


test('preserves independent site edits, deletions and new files across repeated publications', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'durable-db-publish-test-'));
  try {
    const source = join(scratch, 'source');
    const site = join(scratch, 'site');
    mkdirSync(source);
    mkdirSync(site);
    const run = (command, args) => {
      const result = spawnSync(command, args, { encoding: 'utf8', timeout: 5000 });
      assert.ifError(result.error);
      assert.equal(result.status, 0, result.stderr);
    };
    const git = (...args) => run('git', ['-C', site, ...args]);
    git('init', '-q');
    const original = { 'index.html': 'old home', 'recovery.html': 'old docs',
      'style.css': 'old style', 'retired.html': 'obsolete', 'deleted.html': 'removed' };
    for (const [name, value] of Object.entries(original)) writeFileSync(join(site, name), value);
    git('add', '.');
    git('-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid',
      'commit', '-qm', `Publish docs for ${commit}`);
    writeFileSync(join(site, 'index.html'), 'independent home');
    writeFileSync(join(site, 'style.css'), 'independent style');
    rmSync(join(site, 'deleted.html'));
    writeFileSync(join(site, 'new.html'), 'independent collision');
    writeFileSync(join(site, 'custom.txt'), 'independent extra');
    for (const [name, value] of Object.entries({ ...original, 'new.html': 'new', 'evidence.json': '{}' })) {
      if (name !== 'retired.html') writeFileSync(join(source, name), `updated ${value}`);
    }
    for (const sourceCommit of ['b'.repeat(40), 'c'.repeat(40)]) {
      run('python3', ['scripts/merge-docs.py', source, site, sourceCommit]);
      const marker = JSON.parse(readFileSync(join(site, 'deployment.json'), 'utf8'));
      assert.equal(marker.source_commit, sourceCommit);
      assert.deepEqual(marker.preserved_overrides, ['deleted.html', 'index.html', 'new.html', 'style.css']);
      assert.equal(readFileSync(join(site, 'index.html'), 'utf8'), 'independent home');
      assert.equal(readFileSync(join(site, 'style.css'), 'utf8'), 'independent style');
      assert.equal(readFileSync(join(site, 'recovery.html'), 'utf8'), 'updated old docs');
      assert.equal(readFileSync(join(site, 'new.html'), 'utf8'), 'independent collision');
      assert.equal(readFileSync(join(site, 'custom.txt'), 'utf8'), 'independent extra');
      assert.equal(existsSync(join(site, 'deleted.html')), false);
      assert.equal(existsSync(join(site, 'retired.html')), false);
    }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});
