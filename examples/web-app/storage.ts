/** Server-side application storage. Database credentials never go to the browser. */
import { createHash, randomUUID } from 'node:crypto';
import { DurableDb, DbError, encode, decode } from '../../clients/javascript/client.mjs';
import type { CommitRequest, Write } from '../../clients/javascript/client.mjs';

export interface Task {
  id: string;
  title: string;
  completed: boolean;
}
export interface Settings {
  theme: 'light' | 'dark';
}
export interface PageCursor {
  after: string;
  revision: number;
}
interface Receipt {
  fingerprint: string;
  result: Task;
}

const encodeJson = (value: unknown): string => encode(JSON.stringify(value));
const decodeJson = (value: string): unknown => JSON.parse(new TextDecoder().decode(decode(value)));
const sleep = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));
const taskKey = (id: string) => `tasks/${id}`;
const openKey = (id: string) => `open/${id}`;

function identifier(id: string): void {
  if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(id)) {
    throw new Error('Expected a UUID operation/task identity');
  }
}
function task(value: unknown): Task {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('id' in value) ||
    typeof value.id !== 'string' ||
    !('title' in value) ||
    typeof value.title !== 'string' ||
    !('completed' in value) ||
    typeof value.completed !== 'boolean'
  ) {
    throw new Error('Invalid stored task');
  }
  identifier(value.id);
  return { id: value.id, title: value.title, completed: value.completed };
}
function receipt(value: unknown): Receipt {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('fingerprint' in value) ||
    typeof value.fingerprint !== 'string' ||
    !('result' in value)
  ) {
    throw new Error('Invalid stored operation receipt');
  }
  return { fingerprint: value.fingerprint, result: task(value.result) };
}

/** Keep the requestId when reporting this error: its operation may have committed. */
export class OperationPending extends Error {
  requestId: string;
  constructor(requestId: string, cause: unknown) {
    super('Operation outcome is unresolved; retry the same application intent', { cause });
    this.requestId = requestId;
  }
}

export class TaskStore {
  #database: DurableDb;
  constructor(database: DurableDb) {
    this.#database = database;
  }

  async settings(): Promise<{ value: Settings; revision: number }> {
    const snapshot = await this.#database.read({ keys: ['settings'] });
    const value = snapshot.values[0] === null ? { theme: 'light' } : decodeJson(snapshot.values[0]);
    if (
      typeof value !== 'object' ||
      value === null ||
      !('theme' in value) ||
      (value.theme !== 'light' && value.theme !== 'dark')
    )
      throw new Error('Invalid settings');
    return { value: { theme: value.theme }, revision: snapshot.revision };
  }

  /** An optimistic edit: return 409 to the UI if its loaded revision is stale. */
  prepareSettings(value: Settings, revision: number, requestId: string): CommitRequest {
    identifier(requestId);
    if (value.theme !== 'light' && value.theme !== 'dark') throw new Error('Invalid theme');
    return {
      expected_revision: revision,
      request_id: requestId,
      writes: [{ key: 'settings', value: encodeJson(value) }],
    };
  }

  /** Send the saved request again unchanged after uncertainty; never rebuild it silently. */
  saveSettings(request: CommitRequest) {
    return this.#database.commit(request);
  }

  async listOpen(cursor?: PageCursor, limit = 20): Promise<{ tasks: Task[]; next?: PageCursor }> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 100)
      throw new Error('Page size must be 1–100');
    const snapshot = await this.#database.read({
      expected_revision: cursor?.revision,
      scans: [{ prefix: 'open/', after: cursor?.after, limit }],
    });
    const page = snapshot.scans[0];
    const ids = page.items.map((item) => item.key.slice('open/'.length));
    ids.forEach(identifier);
    const records =
      ids.length === 0
        ? []
        : (
            await this.#database.read({
              expected_revision: snapshot.revision,
              keys: ids.map(taskKey),
            })
          ).values;
    const tasks = records.map((value) => {
      if (value === null) throw new Error('Task index points to a missing record');
      return task(decodeJson(value));
    });
    return {
      tasks,
      next: page.next === null ? undefined : { after: page.next, revision: snapshot.revision },
    };
  }

  createTask(requestId: string, title: string): Promise<Task> {
    identifier(requestId);
    if (
      typeof title !== 'string' ||
      title.trim().length === 0 ||
      new TextEncoder().encode(title).length > 200
    ) {
      throw new Error('A task title must contain 1–200 UTF-8 bytes');
    }
    // A stable task identity makes API retries and URLs straightforward.
    const result = { id: requestId, title: title.trim(), completed: false };
    return this.#applyOnce(
      requestId,
      { action: 'create', task: result },
      taskKey(result.id),
      (previous) => {
        if (previous !== null) throw new Error('Task identity already exists');
        return {
          result,
          writes: [
            { key: taskKey(result.id), value: encodeJson(result) },
            { key: openKey(result.id), value: encode(result.id) },
          ],
        };
      },
    );
  }

  completeTask(requestId: string, id: string): Promise<Task> {
    identifier(requestId);
    identifier(id);
    return this.#applyOnce(requestId, { action: 'complete', id }, taskKey(id), (previous) => {
      if (previous === null) throw new Error('Task does not exist');
      const result = { ...task(decodeJson(previous)), completed: true };
      return {
        result,
        writes: [
          { key: taskKey(id), value: encodeJson(result) },
          { key: openKey(id), value: null },
        ],
      };
    });
  }

  async #applyOnce(
    requestId: string,
    intent: unknown,
    recordKey: string,
    prepare: (previous: string | null) => { result: Task; writes: Write[] },
  ): Promise<Task> {
    const receiptKey = `operations/${requestId}`;
    const fingerprint = createHash('sha256').update(JSON.stringify(intent)).digest('hex');
    let lastError: unknown;
    for (let attempt = 0; attempt < 8; attempt++) {
      try {
        const snapshot = await this.#database.read({
          keys: [receiptKey, recordKey],
        });
        if (snapshot.values[0] !== null) {
          const stored = receipt(decodeJson(snapshot.values[0]));
          if (stored.fingerprint !== fingerprint)
            throw new Error('Operation identity reused with different input');
          return stored.result;
        }
        const prepared = prepare(snapshot.values[1]);
        await this.#database.commit({
          expected_revision: snapshot.revision,
          request_id: randomUUID(),
          checks: [{ key: receiptKey, value: null }],
          writes: [
            ...prepared.writes,
            {
              key: receiptKey,
              value: encodeJson({ fingerprint, result: prepared.result }),
            },
          ],
        });
        return prepared.result;
      } catch (error) {
        // The application receipt and effect publish atomically. Retrying this
        // intent is safe even if an older HTTP attempt later finishes: only one
        // transaction can observe an absent receipt at its committed revision.
        if (
          error instanceof DbError
            ? ![409, 429, 500, 502, 503, 504].includes(error.status)
            : !(
                error instanceof TypeError ||
                error instanceof DOMException ||
                error instanceof SyntaxError
              )
        )
          throw error;
        lastError = error;
        await sleep(Math.min(1000, 50 * 2 ** attempt));
      }
    }
    throw new OperationPending(requestId, lastError);
  }
}
