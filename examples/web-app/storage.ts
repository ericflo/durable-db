import type { Database } from '@durable-db/client';

export type Task = { id: string; title: string; done: boolean };
export type Stats = { created: number; done: number };

const taskKey = (id: string) => `tasks/${id}`;

export function taskStore(db: Database) {
  return {
    /** Twenty tasks in creation order. Pass the returned cursor to get the next twenty. */
    async list(cursor?: string) {
      const page = await db.list<Task>({ prefix: 'tasks/', limit: 20, cursor });
      return { tasks: page.items.map((item) => item.value), cursor: page.cursor };
    },

    /** Number the task and count it, in one transaction. */
    create(title: string) {
      return db.transaction(async (tx) => {
        const stats = (await tx.get<Stats>('stats')) ?? { created: 0, done: 0 };
        const task: Task = { id: String(stats.created + 1).padStart(8, '0'), title, done: false };
        tx.set(taskKey(task.id), task);
        tx.set('stats', { ...stats, created: stats.created + 1 });
        return task;
      });
    },

    /** Mark a task done and count it, once, however often and concurrently this is called. */
    complete(id: string) {
      return db.transaction(async (tx) => {
        const task = await tx.get<Task>(taskKey(id));
        if (task === undefined || task.done) return task;
        const stats = (await tx.get<Stats>('stats'))!;
        const done = { ...task, done: true };
        tx.set(taskKey(id), done);
        tx.set('stats', { ...stats, done: stats.done + 1 });
        return done;
      });
    },

    async stats() {
      return (await db.get<Stats>('stats')) ?? { created: 0, done: 0 };
    },
  };
}
