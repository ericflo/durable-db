import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

// Exercise the actual shell script with isolated transport/clock doubles.
// No credentials, network, repository mutation or real waiting are involved.
const commit = 'a'.repeat(40);
for (const [name, mode, source, status] of [
  ['confirms the public source marker', 'match', commit, 0],
  ['rejects stale Pages after a successful push', 'stale', commit, 1],
  ['rejects an HTTP failure even with a matching body', 'error', commit, 1],
  ['rejects an invalid source identity before publishing', 'match', 'invalid', 2],
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
      writeFileSync(clock, '0\n');
      const shims = {
        git: `
if [ "$1" = clone ]; then
  for destination do :; done
  mkdir -p "$destination"
fi
if [ "$1" = -C ]; then shift 2; fi
if [ "$1" = diff ]; then exit 1; fi
exit 0`,
        curl: `
for output do :; done
if [ "$MOCK_MODE" = stale ]; then
  printf '{"source_commit":"stale"}\\n' > "$output"
else
  printf '{"source_commit":"%s"}\\n' "$CI_COMMIT_SHA" > "$output"
fi
if [ "$MOCK_MODE" = error ]; then exit 22; fi`,
        date: 'cat "$MOCK_CLOCK"',
        sleep: 'printf "700\\n" > "$MOCK_CLOCK"',
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
        },
      });
      assert.ifError(result.error);
      assert.equal(result.status, status, result.stderr);
      assert.equal(result.stdout.includes('Public Pages deployment verified'), status === 0);
    } finally {
      rmSync(scratch, { recursive: true, force: true });
    }
  });
}
