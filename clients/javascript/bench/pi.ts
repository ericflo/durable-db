/**
 * pi-durable's own storage benchmarks against the adapter on a local server, with pi-durable's
 * in-memory storage beside it for reference. Run: npm run bench
 */
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import { MemoryStorage, type Storage } from '@earendil-works/pi-durable';
import {
  STORAGE_READ_BENCHMARKS,
  STORAGE_WRITE_BENCHMARKS,
  TIMING_SCALE,
  seedStorageBenchmark,
  seedStorageWriteBenchmark,
  storageBenchmarkPrimaryRecordCount,
} from '@earendil-works/pi-durable/testing';
import { openPiStorage } from '../src/pi/index.ts';
import { startServer } from '../src/testing.ts';

const SAMPLES = 200;
const BUDGET_MS = 1000;

interface Row {
  name: string;
  median: number;
  p95: number;
  requests: number;
  memory: number;
}

let requests = 0;
const counting: typeof fetch = (input, init) => {
  requests++;
  return fetch(input, init);
};

/** Median and 95th percentile milliseconds of `run`, and HTTP requests per call. */
async function measure(run: () => Promise<number>, expected: number) {
  for (let warm = 0; warm < 3; warm++) await run();
  const times: number[] = [];
  const before = requests;
  const deadline = performance.now() + BUDGET_MS;
  while (times.length < SAMPLES && performance.now() < deadline) {
    const started = performance.now();
    const result = await run();
    times.push(performance.now() - started);
    if (result !== expected) throw new Error(`benchmark returned ${result}, expected ${expected}`);
  }
  times.sort((a, b) => a - b);
  return {
    median: times[Math.floor(times.length / 2)]!,
    p95: times[Math.min(times.length - 1, Math.floor(times.length * 0.95))]!,
    requests: (requests - before) / times.length,
  };
}

const built = fileURLToPath(new URL('../../../target/release/durable-db', import.meta.url));
const server = await startServer({
  binary: process.env.DURABLE_DB_BINARY ?? (existsSync(built) ? built : undefined),
});
try {
  const db = await server.createDatabase('bench', { fetch: counting });
  const rows: Row[] = [];

  const adapter = await openPiStorage(db, { prefix: 'read/' });
  const memory: Storage = new MemoryStorage();
  const started = performance.now();
  const dataset = await seedStorageBenchmark(adapter, TIMING_SCALE);
  const seeded = performance.now() - started;
  const seedRequests = requests;
  const memoryDataset = await seedStorageBenchmark(memory, TIMING_SCALE);
  for (const benchmark of STORAGE_READ_BENCHMARKS) {
    const measured = await measure(
      () => benchmark.run(adapter, dataset),
      benchmark.expected(dataset),
    );
    const reference = await measure(
      () => benchmark.run(memory, memoryDataset),
      benchmark.expected(memoryDataset),
    );
    rows.push({ name: benchmark.name, ...measured, memory: reference.median });
  }
  await adapter.close(BACKGROUND_CONTEXT);

  for (const benchmark of STORAGE_WRITE_BENCHMARKS) {
    const target = await openPiStorage(db, { prefix: `write-${rows.length}/` });
    const reference: Storage = new MemoryStorage();
    await seedStorageWriteBenchmark(target);
    await seedStorageWriteBenchmark(reference);
    const measured = await measure(() => benchmark.run(target), benchmark.expected);
    const baseline = await measure(() => benchmark.run(reference), benchmark.expected);
    rows.push({ name: benchmark.name, ...measured, memory: baseline.median });
    await target.close(BACKGROUND_CONTEXT);
  }

  const ms = (value: number) => value.toFixed(value < 0.1 ? 4 : 2).padStart(9);
  const width = Math.max(...rows.map((row) => row.name.length));
  console.log(
    `durable-db pi-durable storage adapter, local server with --no-fsync, Node ${process.version}`,
  );
  console.log(
    `seed: ${storageBenchmarkPrimaryRecordCount(TIMING_SCALE)} records, ` +
      `${seedRequests} requests, ${(seeded / 1000).toFixed(1)} s\n`,
  );
  console.log(`${'benchmark'.padEnd(width)}  median ms    p95 ms  requests  memory ms`);
  for (const row of rows)
    console.log(
      `${row.name.padEnd(width)}  ${ms(row.median)} ${ms(row.p95)} ${row.requests.toFixed(1).padStart(9)}  ${ms(row.memory)}`,
    );
} finally {
  await server.stop();
}
