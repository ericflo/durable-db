/** Deterministic Pi Harness fixture. No provider credentials or network model calls. */
import { BACKGROUND_CONTEXT, awaitWithContext } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { Harness, createRegistry, defineExtension, defineTool } from '@earendil-works/pi-durable';
import { DurableDb } from './client.mjs';
import { openIndexedPiStorage } from './pi-indexed.mjs';

export const context = BACKGROUND_CONTEXT;
export const answer = text => fauxAssistantMessage(text);
export const toolCall = () => fauxAssistantMessage(fauxToolCall('fixture', { value: 'durable-tool' }), { stopReason: 'toolUse' });
export function gate() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
export function routedClient({ urls, database, token }) {
  const clients = urls.map(url => new DurableDb({ url, database, token }));
  const calls = urls.map(() => 0); let index = 0;
  const choose = () => { const current = index++ % clients.length; calls[current]++; return clients[current]; };
  return { read: body => choose().read(body), commit: body => choose().commit(body), calls };
}
export async function openHarness(client, { takeover = false, responses = [], execute, replay = 'safe', onReport = () => {} } = {}) {
  const faux = fauxProvider();
  faux.setResponses(responses);
  const models = createModels(); models.setProvider(faux.provider);
  const registry = createRegistry();
  const tool = defineTool({ name: 'fixture', description: 'Deterministic qualification tool', replay,
    parameters: { type: 'object', properties: { value: { type: 'string' } }, required: ['value'], additionalProperties: false },
    execute: execute ?? (async args => ({ output: [{ type: 'text', text: args.value }] })) });
  registry.install(defineExtension({ name: 'qualification', tools: [tool] }));
  const storage = await openIndexedPiStorage(client, { takeover });
  const harness = await Harness.open(storage, { models, registry, onReport,
    settings: { progress: { partialIntervalMs: 500, outputIntervalMs: 500, persistWithinMs: 1000 } } }, context);
  const model = faux.getModel();
  const root = await harness.root(context, { agent: { model: { provider: model.provider, modelId: model.id }, tools: [tool] } });
  return { harness, root, faux, storage };
}
export async function blockedTool(entered, release, args, _api, callContext) {
  entered.resolve();
  await awaitWithContext(release.promise, callContext);
  return { output: [{ type: 'text', text: args.value }] };
}
export async function entries(root) {
  const result = []; let cursor;
  do {
    const page = await root.entries({ order: 'ascending' }, 100, cursor, context);
    result.push(...page.items); cursor = page.next;
  } while (cursor !== undefined);
  return result;
}
