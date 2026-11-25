import { describe, expect, it } from 'vitest';
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import { ROOT_CONVERSATION_ID } from '@earendil-works/pi-durable';
import type { EntryId, Seq, Storage, StorageWrite, TaskId } from '@earendil-works/pi-durable';
import { commitWrites } from '@earendil-works/pi-durable/testing';
import { openPiStorage } from '../src/pi/index.ts';
import { createDatabase, recordingFetch, type Call } from './fixture.ts';

const context = BACKGROUND_CONTEXT;
const root: StorageWrite = { type: 'conversation', value: { id: ROOT_CONVERSATION_ID } };
const entry = (
  id: EntryId,
  data?: unknown,
  conversationId = ROOT_CONVERSATION_ID,
): StorageWrite => ({
  type: 'entry',
  value: {
    id,
    conversationId,
    kind: 'note',
    ...(data === undefined ? {} : { data: data as never }),
  },
});
const task = (id: TaskId, status: 'pending' | 'running', kind = 'test.task'): StorageWrite => ({
  type: 'task',
  value: {
    id: id as TaskId<never>,
    conversationId: ROOT_CONVERSATION_ID,
    kind,
    version: 1,
    input: {},
    state: { status, checkpoint: { phase: 'ready' } },
    background: false,
    abortRequested: false,
  },
});

/** A storage whose HTTP requests are recorded and can be intercepted. */
async function opened(
  intercept?: (call: Call, send: () => Promise<Response>) => Promise<Response>,
) {
  const created = await createDatabase();
  const recorder = recordingFetch(intercept);
  const db = created.open({ fetch: recorder.fetch, maxRetries: 2 });
  const storage = await openPiStorage(db);
  return { ...created, db, storage, calls: recorder.calls };
}

/** The paths of the requests `operation` sends. */
async function requests(calls: Call[], operation: () => Promise<unknown>): Promise<string[]> {
  const before = calls.length;
  await operation();
  return calls.slice(before).map((call) => call.path);
}

describe('ownership', () => {
  it('keeps records, sequences and minted IDs across close and reopen', async () => {
    const { db, storage } = await opened();
    const first = await storage.mintId<EntryId>();
    const seq = await commitWrites(storage, [root, entry(first, { text: 'kept' })], context);
    const unstored = await storage.mintId<EntryId>();
    await storage.close(context);

    const reopened = await openPiStorage(db);
    expect((await reopened.entry(first, context))?.entry.data).toEqual({ text: 'kept' });
    const next = await reopened.mintId<EntryId>();
    expect(next).toBeGreaterThan(unstored);
    expect(await commitWrites(reopened, [entry(next)], context)).toBeGreaterThan(seq);
    await reopened.close(context);
  });

  it('refuses a second owner until takeover, which fences the first', async () => {
    const { db, open, storage: first } = await opened();
    const id = await first.mintId<EntryId>();
    await commitWrites(first, [root, entry(id)], context);
    await expect(openPiStorage(db)).rejects.toThrow('already has an owner');

    const second = await openPiStorage(open(), { takeover: true });
    await expect(
      commitWrites(first, [entry(await first.mintId<EntryId>())], context),
    ).rejects.toThrow('taken over');
    await expect(first.entry(id, context)).rejects.toThrow('failed');
    // Closing the fenced storage must not release the new owner's claim.
    await first.close(context);
    await expect(openPiStorage(db)).rejects.toThrow('already has an owner');

    expect((await second.entry(id, context))?.entry.id).toBe(id);
    const next = await second.mintId<EntryId>();
    expect(next).toBeGreaterThan(id);
    await commitWrites(second, [entry(next)], context);
    expect(
      (await second.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 10, undefined, context))
        .items,
    ).toHaveLength(2);
    await second.close(context);
  });

  it('notices a takeover on the next read', async () => {
    const { open, storage: first } = await opened();
    await commitWrites(first, [root], context);
    const second = await openPiStorage(open(), { takeover: true });
    await expect(first.conversation(ROOT_CONVERSATION_ID, context)).rejects.toThrow('taken over');
    await second.close(context);
  });

  it('keeps storages under different prefixes apart', async () => {
    const { db } = await createDatabase();
    const a = await openPiStorage(db, { prefix: 'a/' });
    const b = await openPiStorage(db, { prefix: 'b/' });
    await commitWrites(a, [root], context);
    expect(await b.conversation(ROOT_CONVERSATION_ID, context)).toBeUndefined();
    expect(await b.mintId()).toBe(2);
    await Promise.all([a.close(context), b.close(context)]);
    await db.set('other/key', true);
    await expect(openPiStorage(db, { prefix: 'other/' })).rejects.toThrow('not a Pi storage');
    await expect(openPiStorage(db, { prefix: 'bad prefix' })).rejects.toThrow(TypeError);
  });

  it('finishes operations underway before close releases ownership', async () => {
    const { db, storage } = await opened();
    const id = await storage.mintId<EntryId>();
    const committing = commitWrites(storage, [root, entry(id)], context);
    const reading = storage.conversation(ROOT_CONVERSATION_ID, context);
    const closing = storage.close(context);
    await expect(storage.mintId()).rejects.toThrow('closed');
    await expect(committing).resolves.toBe(1);
    // The read ran beside the commit, so it saw the state before it or after it.
    expect([undefined, { id: ROOT_CONVERSATION_ID }]).toContainEqual(await reading);
    await closing;
    await storage.close(context);
    const reopened = await openPiStorage(db);
    expect(await reopened.entry(id, context)).toBeDefined();
    await reopened.close(context);
  });
});

describe('failures', () => {
  it('stores a commit once when its response is lost', async () => {
    let lose = false;
    const { storage, calls } = await opened(async (call, send) => {
      if (!lose || call.path !== '/commit') return send();
      lose = false;
      await send();
      throw new TypeError('socket closed');
    });
    await commitWrites(storage, [root], context);
    const id = await storage.mintId<EntryId>();
    lose = true;
    const sent = await requests(calls, async () => {
      expect(await commitWrites(storage, [entry(id)], context)).toBe(2);
    });
    expect(sent).toEqual(['/commit', '/commit']);
    expect(calls.at(-1)!.body).toEqual(calls.at(-2)!.body);
    const page = await storage.scanEntries(
      { conversationId: ROOT_CONVERSATION_ID },
      10,
      undefined,
      context,
    );
    expect(page.items.map((item) => item.id)).toEqual([id]);
    expect(await commitWrites(storage, [entry(await storage.mintId<EntryId>())], context)).toBe(3);
    await storage.close(context);
  });

  it('rejects a commit whose outcome is unknown and fails until reopened with takeover', async () => {
    let down = false;
    const { db, storage } = await opened(async (call, send) => {
      if (down && call.path === '/commit') throw new TypeError('connection refused');
      return send();
    });
    await commitWrites(storage, [root], context);
    down = true;
    const receipts: Seq[] = [];
    const unknown = storage.commit(
      [[entry(await storage.mintId<EntryId>())]],
      (_index, seq) => receipts.push(seq),
      context,
    );
    await expect(unknown).rejects.toMatchObject({ code: 'network', outcomeUnknown: true });
    expect(receipts).toEqual([]);
    await expect(storage.conversation(ROOT_CONVERSATION_ID, context)).rejects.toThrow('failed');
    down = false;
    await storage.close(context);
    await expect(openPiStorage(db)).rejects.toThrow('already has an owner');
    const reopened = await openPiStorage(db, { takeover: true });
    expect(await reopened.conversation(ROOT_CONVERSATION_ID, context)).toEqual({
      id: ROOT_CONVERSATION_ID,
    });
    await reopened.close(context);
  });

  it('stays usable after a commit the server definitely refused', async () => {
    const { db, storage } = await opened();
    const id = await storage.mintId<EntryId>();
    await commitWrites(storage, [root], context);
    const receipts: number[] = [];
    // The second batch holds a value over the server's 4 MiB limit, so the request is invalid
    // and nothing in it is stored.
    const oversized = storage.commit(
      [[entry(id)], [entry(await storage.mintId<EntryId>(), 'x'.repeat(5 * 1024 * 1024))]],
      (index) => receipts.push(index),
      context,
    );
    await expect(oversized).rejects.toMatchObject({ status: 400, outcomeUnknown: false });
    expect(receipts).toEqual([]);
    expect(await storage.entry(id, context)).toBeUndefined();
    expect(await commitWrites(storage, [entry(id)], context)).toBe(2);
    await storage.close(context);
    await (await openPiStorage(db)).close(context);
  });

  it('rejects, not refuses, when a read inside a commit fails', async () => {
    let down = false;
    const { storage } = await opened(async (call, send) => {
      if (down && call.path === '/read') throw new TypeError('connection refused');
      return send();
    });
    const id = await storage.mintId<TaskId>();
    await commitWrites(storage, [root, task(id, 'pending')], context);
    down = true;
    // Rewriting a task reads its stored record. That read failing is not a verdict on the batch.
    await expect(storage.commit([[task(id, 'running')]], () => {}, context)).rejects.toMatchObject({
      code: 'network',
    });
    down = false;
    expect((await storage.task(id, context))?.state.status).toBe('pending');
    await storage.close(context);
  });
});

describe('reads beside a commit', () => {
  it('answers a read while a commit is still being written', async () => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    let hold = false;
    const { storage } = await opened(async (call, send) => {
      if (hold && call.path === '/commit') await held;
      return send();
    });
    const id = await storage.mintId<EntryId>();
    await commitWrites(storage, [root, entry(id)], context);
    hold = true;
    let committed = false;
    const committing = commitWrites(
      storage,
      [entry(await storage.mintId<EntryId>())],
      context,
    ).then(() => {
      committed = true;
    });
    expect((await storage.entry(id, context))?.entry.id).toBe(id);
    expect(
      (await storage.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 10, undefined, context))
        .items,
    ).toHaveLength(1);
    expect(committed).toBe(false);
    release();
    await committing;
    expect(
      (await storage.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 10, undefined, context))
        .items,
    ).toHaveLength(2);
    await storage.close(context);
  });

  it('repeats a read of several requests that a commit was written between', async () => {
    // A status scan reads the index, then the records. Here a commit moves a task out of the
    // status after the index was read and before the records are.
    let commitBetween: (() => Promise<unknown>) | undefined;
    const { storage, calls } = await opened(async (call, send) => {
      if (call.path === '/read' && call.body.keys.length === 3 && commitBetween !== undefined) {
        const commit = commitBetween;
        commitBetween = undefined;
        await commit();
      }
      return send();
    });
    const [first, second] = [await storage.mintId<TaskId>(), await storage.mintId<TaskId>()];
    await commitWrites(storage, [root, task(first, 'pending'), task(second, 'pending')], context);
    commitBetween = () => commitWrites(storage, [task(first, 'running')], context);
    const sent = await requests(calls, async () => {
      const page = await storage.scanTasks({ status: 'pending' }, 10, undefined, context);
      expect(page.items.map((item) => [item.id, item.state.status])).toEqual([[second, 'pending']]);
    });
    // The scan's two reads, the commit's read and write between them, then the scan again.
    expect(sent).toEqual(['/read', '/read', '/read', '/commit', '/read', '/read']);
    await storage.close(context);
  });
});

describe('round trips', () => {
  it('opens in three requests and serves hot operations in one', async () => {
    const { storage, calls } = await opened();
    expect(calls.map((call) => call.path)).toEqual(['/read', '/commit', '/read']);
    expect(calls[0]!.body.consistency).toBeUndefined();
    expect(calls[2]!.body).toMatchObject({ consistency: 'session', min_revision: 1 });

    const ids = await Promise.all(Array.from({ length: 6 }, () => storage.mintId<EntryId>()));
    const [first, second, taskId, child, forked, document] = ids as [
      EntryId,
      EntryId,
      TaskId,
      never,
      EntryId,
      never,
    ];
    // ID 1 is reserved, not minted, so storing the root conversation checks that it is free.
    expect(await requests(calls, () => commitWrites(storage, [root], context))).toEqual([
      '/read',
      '/commit',
    ]);
    // Minted IDs are known to be unused, so a commit of new records reads nothing.
    expect(
      await requests(calls, () =>
        commitWrites(storage, [entry(first), entry(second), task(taskId, 'pending')], context),
      ),
    ).toEqual(['/commit']);
    // Several batches are one request with one transaction each.
    expect(
      await requests(calls, () =>
        storage.commit(
          [
            [
              {
                type: 'conversation',
                value: { id: child, parent: { conversationId: ROOT_CONVERSATION_ID, at: first } },
              },
            ],
            [entry(forked, undefined, child)],
          ],
          () => {},
          context,
        ),
      ),
    ).toEqual(['/commit']);
    expect(calls.at(-1)!.body.transactions).toHaveLength(2);
    // Rewriting a record reads the stored one first, for its indexes.
    expect(
      await requests(calls, () => commitWrites(storage, [task(taskId, 'running')], context)),
    ).toEqual(['/read', '/commit']);
    expect(
      await requests(calls, () =>
        commitWrites(
          storage,
          [
            {
              type: 'document.create',
              record: { id: document, kind: 'settings', scope: { kind: 'session' } },
              content: { kind: 'base', version: 1, value: { theme: 'dark' } },
            },
          ],
          context,
        ),
      ),
    ).toEqual(['/read', '/commit']);

    const one = async (operation: () => Promise<unknown>) =>
      expect(await requests(calls, operation)).toEqual(['/read']);
    await one(() => storage.entry(first, context));
    await one(() => storage.entry(child, first, context));
    await one(() =>
      storage.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 100, undefined, context),
    );
    await one(async () => {
      const page = await storage.scanEntries({ conversationId: child }, 100, undefined, context);
      expect(page.items.map((item) => item.id)).toEqual([forked, first]);
    });
    await one(() => storage.findLatestHeadMarker(child, undefined, context));
    await one(() => storage.task(taskId, context));
    await one(() =>
      storage.findDocument({ kind: 'settings', scope: { kind: 'session' } }, 'current', context),
    );
    await one(async () =>
      expect((await storage.document(document, 'current', context))?.value).toEqual({
        theme: 'dark',
      }),
    );
    expect(
      await requests(calls, () => storage.scanTasks({ status: 'running' }, 10, undefined, context)),
    ).toEqual(['/read', '/read']);
    expect(
      calls.every(
        (call) =>
          call.path === '/commit' || call.body.consistency === 'session' || call === calls[0],
      ),
    ).toBe(true);
    await storage.close(context);
  });
});

describe('sizes', () => {
  it('stores entries of megabytes whole and scans past the 8 MiB page limit', async () => {
    const { storage } = await opened();
    const ids = await Promise.all(Array.from({ length: 5 }, () => storage.mintId<EntryId>()));
    const text = (id: number) => String(id % 10).repeat(2 * 1024 * 1024);
    const result = await storage.commit(
      [[root], ...ids.map((id) => [entry(id, { text: text(id) })])],
      () => {},
      context,
    );
    expect(result).toEqual({ kind: 'stored', stored: 6 });
    const stored = await storage.entry(ids[2]!, context);
    expect((stored!.entry.data as { text: string }).text).toBe(text(ids[2]!));
    const page = await storage.scanEntries(
      { conversationId: ROOT_CONVERSATION_ID, order: 'ascending' },
      4,
      undefined,
      context,
    );
    expect(page.items.map((item) => item.id)).toEqual(ids.slice(0, 4));
    expect(page.next).toEqual({ after: ids[3], order: 'ascending' });
    await storage.close(context);
  });

  it('pages a filtered scan across many records', async () => {
    const { storage } = await opened();
    const ids = await Promise.all(Array.from({ length: 700 }, () => storage.mintId<TaskId>()));
    await commitWrites(
      storage,
      [root, ...ids.map((id, index) => task(id, 'pending', index % 7 === 0 ? 'rare' : 'common'))],
      context,
    );
    const found: number[] = [];
    let cursor;
    do {
      const page = await storage.scanTasks(
        { kind: 'rare', status: 'pending' },
        30,
        cursor,
        context,
      );
      found.push(...page.items.map((item) => item.id));
      cursor = page.next;
    } while (cursor !== undefined);
    expect(found).toEqual(ids.filter((_, index) => index % 7 === 0));
    await storage.close(context);
  });
});
