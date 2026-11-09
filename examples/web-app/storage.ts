import { Database } from '@durable-db/client';

export type Task = { id: string; title: string; completed: boolean };
export type Settings = { theme: 'light' | 'dark' };

export function taskStore(db: Database) {
  return {
    async settings() {
      return (await db.get<Settings>('settings')) ?? { theme: 'light' as const };
    },

    saveSettings(settings: Settings) {
      return db.set('settings', settings);
    },

    list(cursor?: string) {
      return db.list<Task>({ prefix: 'tasks/', limit: 20, cursor });
    },

    create(id: string, title: string) {
      return db.transaction(async (tx) => {
        const existing = await tx.get<Task>(`tasks/${id}`);
        if (existing) return existing;

        const task = { id, title, completed: false };
        tx.set(`tasks/${id}`, task);
        return task;
      });
    },

    complete(id: string) {
      return db.transaction(async (tx) => {
        const task = await tx.get<Task>(`tasks/${id}`);
        if (!task || task.completed) return task;

        const completed = { ...task, completed: true };
        const count = (await tx.get<number>('stats/completed')) ?? 0;
        tx.set(`tasks/${id}`, completed);
        tx.set('stats/completed', count + 1);
        return completed;
      });
    },

    async completedCount() {
      return (await db.get<number>('stats/completed')) ?? 0;
    },
  };
}
