/** Child process killed only after Pi's tool intent has been durably committed. */
import { readFile } from 'node:fs/promises';
import { context, routedClient, openHarness, toolCall } from './harness-fixture.mjs';

const config = JSON.parse(await readFile(process.argv[2], 'utf8'));
const keepAlive = setInterval(() => {}, 1000);
try {
  const { root } = await openHarness(routedClient(config), {
    replay: config.replay,
    responses: config.phase === 'model' ? [() => {
      process.send({ phase: 'entered' });
      return new Promise(() => {});
    }] : [toolCall()],
    execute: async () => {
      process.send({ phase: 'entered' });
      return new Promise(() => {});
    },
    onReport: report => process.send({ phase: 'report', kind: report.kind }),
  });
  const submission = await root.submit({ type: 'input', content: 'Run the deterministic tool', requestId: config.requestId }, context);
  process.send({ phase: 'submitted', submission: submission.id, conversation: root.id });
} catch (error) {
  // Fixture errors contain no credentials; send only the category to the parent.
  process.send({ phase: 'failure', name: error.name });
  clearInterval(keepAlive); process.exitCode = 1;
}
