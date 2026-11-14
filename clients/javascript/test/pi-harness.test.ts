/** pi-durable's Harness running on the adapter, with a scripted model: no network, no credentials. */
import { expect, it } from 'vitest';
import { BACKGROUND_CONTEXT, awaitWithContext } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from '@earendil-works/pi-ai/providers/faux';
import { Harness, createRegistry, defineExtension, defineTool } from '@earendil-works/pi-durable';
import type { Conversation, EntryRecord, Report } from '@earendil-works/pi-durable';
import type { Database } from '../src/index.ts';
import { openPiStorage } from '../src/pi/index.ts';
import { createDatabase } from './fixture.ts';

const context = BACKGROUND_CONTEXT;
const toolCall = () =>
  fauxAssistantMessage(fauxToolCall('echo', { value: 'from the tool' }), { stopReason: 'toolUse' });
const input = (requestId: string) => ({
  type: 'input' as const,
  content: 'Run the tool',
  requestId,
});

async function openHarness(
  db: Database,
  options: {
    takeover?: boolean;
    responses?: Parameters<ReturnType<typeof fauxProvider>['setResponses']>[0];
    execute?: (args: { value: string }, callContext: typeof context) => Promise<string>;
    onReport?: (report: Report) => void;
  } = {},
) {
  const faux = fauxProvider();
  faux.setResponses(options.responses ?? []);
  const models = createModels();
  models.setProvider(faux.provider);
  const tool = defineTool({
    name: 'echo',
    description: 'Returns its argument',
    replay: 'safe',
    parameters: {
      type: 'object',
      properties: { value: { type: 'string' } },
      required: ['value'],
      additionalProperties: false,
    },
    execute: async (args: { value: string }, _api, callContext) => ({
      output: [
        {
          type: 'text',
          text: await (options.execute ?? (async (a) => a.value))(args, callContext),
        },
      ],
    }),
  });
  const registry = createRegistry();
  registry.install(defineExtension({ name: 'test', tools: [tool] }));
  const storage = await openPiStorage(db, { takeover: options.takeover });
  const harness = await Harness.open(
    storage,
    { models, registry, onReport: options.onReport ?? (() => {}) },
    context,
  );
  const model = faux.getModel();
  const root = await harness.root(context, {
    agent: { model: { provider: model.provider, modelId: model.id }, tools: [tool] },
  });
  return { harness, root, faux };
}

async function history(root: Conversation): Promise<EntryRecord[]> {
  const entries: EntryRecord[] = [];
  let cursor;
  do {
    const page = await root.entries({ order: 'ascending' }, 100, cursor, context);
    entries.push(...page.items);
    cursor = page.next;
  } while (cursor !== undefined);
  return entries;
}
const kinds = (entries: EntryRecord[], kind: string) =>
  entries.filter((entry) => entry.kind === kind);

it('runs a tool round trip, then answers a repeated request from storage after reopening', async () => {
  const { db } = await createDatabase();
  const first = await openHarness(db, { responses: [toolCall(), fauxAssistantMessage('done')] });
  const requestId = crypto.randomUUID();
  const submission = await first.root.submit(input(requestId), context);
  const settled = await submission.wait(context);
  expect(settled.status).toBe('done');
  expect(first.faux.state.callCount).toBe(2);
  const entries = await history(first.root);
  expect(kinds(entries, 'pi.user')).toHaveLength(1);
  expect(kinds(entries, 'pi.tool-result')).toHaveLength(1);
  expect(kinds(entries, 'pi.tool-result')[0]!.model![0]).toMatchObject({
    content: [{ type: 'text', text: 'from the tool' }],
    isError: false,
  });
  await first.harness.close(context);

  const second = await openHarness(db);
  const repeated = await second.root.submit(input(requestId), context);
  expect(repeated.id).toBe(submission.id);
  expect(await repeated.wait(context)).toEqual(settled);
  expect(second.faux.state.callCount).toBe(0);
  expect(await history(second.root)).toEqual(entries);
  await second.harness.close(context);
});

it('fences a Harness that is still running a tool when another host takes over', async () => {
  const { db, open } = await createDatabase();
  let entered!: () => void;
  let release!: () => void;
  let failed!: () => void;
  const running = new Promise<void>((resolve) => (entered = resolve));
  const blocked = new Promise<void>((resolve) => (release = resolve));
  const storageFailed = new Promise<void>((resolve) => (failed = resolve));
  const old = await openHarness(db, {
    responses: [toolCall()],
    execute: async (args, callContext) => {
      entered();
      await awaitWithContext(blocked, callContext);
      return args.value;
    },
    onReport: (report) => {
      if (report.kind === 'storage_failed') failed();
    },
  });
  const submission = await old.root.submit(input(crypto.randomUUID()), context);
  await running;

  // The replacement host recovers the interrupted tool call and finishes the run.
  const next = await openHarness(open(), {
    takeover: true,
    responses: [fauxAssistantMessage('recovered')],
  });
  const refused = expect(submission.wait(context)).rejects.toThrow();
  release();
  await storageFailed;
  await refused;
  const recovered = await next.harness.submission(submission.id, context);
  expect((await recovered!.wait(context)).status).toBe('done');
  const entries = await history(next.root);
  expect(kinds(entries, 'pi.user')).toHaveLength(1);
  expect(kinds(entries, 'pi.tool-result')).toHaveLength(1);
  await old.harness.close(context);
  await next.harness.close(context);
});
