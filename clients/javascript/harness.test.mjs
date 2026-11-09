import { describe, expect, it } from 'vitest';
import { withCancel } from '@earendil-works/chord/context';
import { readFileSync } from 'node:fs';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { DurableDb } from './client.mjs';
import { retry } from './indexed/kv.mjs';
import { openIndexedPiStorage } from './pi-indexed.mjs';
import { context, gate, routedClient, openHarness, toolCall, answer, blockedTool, entries } from './harness-fixture.mjs';

const urls = (process.env.DURABLE_DB_TEST_URLS ?? process.env.DURABLE_DB_TEST_URL ?? '').split(',').filter(Boolean);
const token = process.env.DURABLE_DB_TEST_TOKEN_FILE ? readFileSync(process.env.DURABLE_DB_TEST_TOKEN_FILE, 'utf8').trim() : '';
const suite = urls.length ? describe : describe.skip;
async function fixture() {
  const database = `harness-${crypto.randomUUID()}`;
  const admin = new DurableDb({ url: urls[0], database, token });
  const creation = { request_id: crypto.randomUUID() };
  await retry(() => admin.createProtected(creation));
  const issued = await retry(() => admin.issueToken({ write: true }));
  const config = { urls, database, token: issued.token };
  return { client: routedClient(config), config };
}
const input = requestId => ({ type: 'input', content: 'Run the deterministic tool', requestId });
const results = history => history.filter(entry => entry.kind === 'pi.tool-result');
const users = history => history.filter(entry => entry.kind === 'pi.user');

suite.sequential('Pi Harness 1.5.0 over the indexed HTTP adapter', () => {
  it('persists a tool round trip and deduplicates input across close and reopen after a lost commit response', async () => {
    const { client } = await fixture(); let lose = false, lost = 0;
    const ambiguous = { read: body => client.read(body), commit: async body => {
      const result = await client.commit(body);
      if (lose) { lose = false; lost++; throw new TypeError('Injected lost acknowledgement'); }
      return result;
    } };
    const first = await openHarness(ambiguous, { responses: [toolCall(), answer('tool completed')] });
    const requestId = crypto.randomUUID(); let settled;
    try {
      lose = true;
      const submission = await first.root.submit(input(requestId), context);
      settled = await submission.wait(context);
      expect(settled.status).toBe('done'); expect(lost).toBe(1);
      const history = await entries(first.root);
      expect(users(history)).toHaveLength(1); expect(results(history)).toHaveLength(1);
      expect(results(history)[0].model[0].content).toEqual([{ type: 'text', text: 'durable-tool' }]);
      expect(first.faux.state.callCount).toBe(2);
    } finally { await first.harness.close(context); }
    const next = await openHarness(client);
    try {
      const repeated = await next.root.submit(input(requestId), context);
      expect(await repeated.wait(context)).toEqual(settled);
      expect(next.faux.state.callCount).toBe(0);
      expect(users(await entries(next.root))).toHaveLength(1);
      expect(client.calls.every(count => count > 0)).toBe(true);
    } finally { await next.harness.close(context); }
  }, 300000);

  it('cancels only a wait, withdraws queued input, and lets the original tool finish', async () => {
    const { client } = await fixture(); const entered = gate(), release = gate();
    const run = await openHarness(client, { responses: [toolCall(), answer('continued after wait cancellation')],
      execute: (...args) => blockedTool(entered, release, ...args) });
    try {
      const submission = await run.root.submit(input(crypto.randomUUID()), context);
      await entered.promise;
      const waiter = withCancel(context), waiting = submission.wait(waiter.context);
      const rejected = expect(waiting).rejects.toThrow(); waiter.cancel(new Error('Wait cancelled')); await rejected;
      const queued = await run.root.submit(input(crypto.randomUUID()), context);
      expect(await queued.abort(context)).toBe('aborted');
      expect((await queued.wait(context)).status).toBe('unanswered');
      release.resolve();
      expect((await submission.wait(context)).status).toBe('done');
      const history = await entries(run.root);
      expect(users(history)).toHaveLength(1); expect(results(history)).toHaveLength(1);
      expect(results(history)[0].model[0].isError).toBe(false);
    } finally { release.resolve(); await run.harness.close(context); }
  }, 300000);

  it('durably aborts an executing tool and preserves the aborted result after reopen', async () => {
    const { client } = await fixture(); const entered = gate(), release = gate();
    const run = await openHarness(client, { responses: [toolCall()], execute: (...args) => blockedTool(entered, release, ...args) });
    let id;
    try {
      const submission = await run.root.submit(input(crypto.randomUUID()), context); id = submission.id;
      await entered.promise; await run.root.abort(context);
      const settled = await submission.wait(context);
      expect(settled.status).toBe('unanswered'); expect(settled.reason).toBe('aborted');
      const result = results(await entries(run.root)); expect(result).toHaveLength(1);
      expect(result[0].model[0].isError).toBe(true);
      expect(result[0].data.diagnostics.some(item => item.code === 'aborted')).toBe(true);
    } finally { release.resolve(); await run.harness.close(context); }
    const next = await openHarness(client);
    try {
      expect((await (await next.harness.submission(id, context)).wait(context)).reason).toBe('aborted');
      expect(next.faux.state.callCount).toBe(0);
    } finally { await next.harness.close(context); }
  }, 300000);

  it('fences a still-running Harness after takeover before it can store a stale tool result', async () => {
    const { client } = await fixture(); const entered = gate(), release = gate(), failed = gate();
    const old = await openHarness(client, { responses: [toolCall()], execute: (...args) => blockedTool(entered, release, ...args),
      onReport: report => { if (report.kind === 'storage_failed') failed.resolve(); } });
    let next;
    try {
      const submission = await old.root.submit(input(crypto.randomUUID()), context);
      await entered.promise;
      next = await openHarness(client, { takeover: true, responses: [answer('replacement completed')] });
      const refused = expect(submission.wait(context)).rejects.toThrow();
      release.resolve(); await failed.promise; await refused;
      const recovered = await next.harness.submission(submission.id, context);
      expect((await recovered.wait(context)).status).toBe('done');
      const history = await entries(next.root);
      expect(users(history)).toHaveLength(1); expect(results(history)).toHaveLength(1);
      expect(results(history)[0].model[0].isError).toBe(false);
    } finally {
      release.resolve(); await old.harness.close(context);
      if (next) await next.harness.close(context);
    }
  }, 300000);

  it.skipIf(!process.env.DURABLE_DB_TEST_CONTROL_FILE)('continues the live Harness after every API process is killed and cold-replaced', async () => {
    const control = JSON.parse(readFileSync(process.env.DURABLE_DB_TEST_CONTROL_FILE, 'utf8'));
    const { client } = await fixture(); const entered = gate(), release = gate();
    const run = await openHarness(client, { responses: [toolCall(), answer('all API processes replaced')],
      execute: (...args) => blockedTool(entered, release, ...args) });
    try {
      const submission = await run.root.submit(input(crypto.randomUUID()), context);
      await entered.promise;
      const before = [...client.calls];
      const restarted = await fetch(control.url + '/restart', { method: 'POST',
        headers: { authorization: `Bearer ${control.token}` }, signal: AbortSignal.timeout(120000) });
      expect(restarted.status).toBe(200);
      expect(await restarted.json()).toEqual({ killed: urls.length, cold_started: urls.length });
      release.resolve();
      expect((await submission.wait(context)).status).toBe('done');
      const history = await entries(run.root);
      expect(users(history)).toHaveLength(1); expect(results(history)).toHaveLength(1);
      expect(client.calls.every((count, index) => count > before[index])).toBe(true);
    } finally { release.resolve(); await run.harness.close(context); }
  }, 300000);

  for (const { phase, replay } of [{ phase: 'tool', replay: 'safe' }, { phase: 'tool', replay: 'unsafe' }, { phase: 'model', replay: 'safe' }])
    it(`recovers a SIGKILL during ${phase === 'model' ? 'a model request' : `a ${replay} tool`} with explicit ownership transfer`, async () => {
    const { client, config } = await fixture(); const dir = await mkdtemp(join(tmpdir(), 'durable-pi-'));
    const requestId = crypto.randomUUID(); let child;
    try {
      const path = join(dir, 'config.json');
      await writeFile(path, JSON.stringify({ ...config, replay, requestId, phase }), { mode: 0o600 });
      child = fork(new URL('./harness-worker.mjs', import.meta.url), [path], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
      const started = new Promise((resolve, reject) => {
        let submitted, entered = false;
        const timer = setTimeout(() => reject(new Error('Harness did not reach its durable intent')), 180000);
        child.on('error', reject);
        child.on('exit', () => { clearTimeout(timer); reject(new Error('Harness worker exited before the fault')); });
        child.on('message', message => {
          if (message.phase === 'failure' || message.phase === 'report') { clearTimeout(timer); reject(new Error(`Harness worker ${message.phase}: ${message.kind ?? message.name}`)); }
          if (message.phase === 'submitted') submitted = message;
          if (message.phase === 'entered') entered = true;
          if (submitted && entered) { clearTimeout(timer); resolve(submitted); }
        });
      });
      const state = await started;
      await expect(openIndexedPiStorage(client)).rejects.toThrow('already has an owner');
      const exited = once(child, 'exit'); child.kill('SIGKILL');
      expect((await exited)[1]).toBe('SIGKILL');
      let executions = 0;
      const next = await openHarness(client, { takeover: true, replay, responses: [answer('recovered tool outcome')],
        execute: async args => { executions++; return { output: [{ type: 'text', text: args.value }] }; } });
      try {
        expect(next.root.id).toBe(state.conversation);
        const submission = await next.harness.submission(state.submission, context);
        expect((await submission.wait(context)).status).toBe('done');
        expect(executions).toBe(phase === 'tool' && replay === 'safe' ? 1 : 0);
        const history = await entries(next.root), result = results(history);
        expect(users(history)).toHaveLength(1); expect(result).toHaveLength(phase === 'tool' ? 1 : 0);
        if (phase === 'tool') expect(result[0].model[0].isError).toBe(replay === 'unsafe');
        if (phase === 'tool' && replay === 'unsafe') expect(result[0].data.diagnostics.some(item => item.code === 'interrupted')).toBe(true);
        const repeated = await next.root.submit(input(requestId), context);
        expect(repeated.id).toBe(state.submission);
        expect(next.faux.state.callCount).toBe(1);
      } finally { await next.harness.close(context); }
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) { const exited = once(child, 'exit'); child.kill('SIGKILL'); await exited; }
      await rm(dir, { recursive: true, force: true });
    }
  }, 300000);
});
