// Check the packaged executable, with no compiler or tools on its PATH.
// Usage: node scripts/check-release.mjs ARCHIVE [VERSION]
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import {
  chmodSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const [input, expected] = process.argv.slice(2);
const version =
  expected ??
  readFileSync(new URL("../Cargo.toml", import.meta.url), "utf8").match(
    /^version = "([^"]+)"/m,
  )[1];
assert(
  input && /^\d+\.\d+\.\d+$/.test(version),
  "expected archive and release version",
);
const archive = resolve(input);
const sha256 = (data) => createHash("sha256").update(data).digest("hex");
const digest = sha256(readFileSync(archive));
const checksum = readFileSync(join(dirname(archive), "SHA256SUMS"), "utf8")
  .split("\n")
  .find((line) => line.trim().endsWith(`  ${basename(archive)}`));
assert.equal(checksum, `${digest}  ${basename(archive)}`);
const members = execFileSync("tar", ["tzf", archive], { encoding: "utf8" })
  .trim()
  .split("\n");
assert.deepEqual(members.sort(), ["LICENSE", "durable-db"]);
const temp = mkdtempSync(join(tmpdir(), "durable-db-release-"));
const binary = join(temp, "durable-db");
const env = { PATH: "", RUST_LOG: "off" };
const run = (...args) =>
  execFileSync(binary, args, {
    env,
    encoding: "utf8",
    timeout: 30_000,
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
let server;
let url;
let token;
async function stop() {
  if (server?.pid && server.exitCode === null && server.signalCode === null) {
    const exited = once(server, "exit");
    server.kill("SIGKILL");
    await exited;
  }
}
async function start() {
  const reservation = createServer().listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  url = `http://127.0.0.1:${port}`;
  server = spawn(
    binary,
    ["serve", "--data", join(temp, "data"), "--listen", `127.0.0.1:${port}`],
    { env, stdio: "ignore" },
  );
  let failure;
  server.on("error", (error) => {
    failure = error;
  });
  for (let attempt = 0; attempt < 100; attempt++) {
    if (failure) throw failure;
    assert.equal(
      server.exitCode,
      null,
      "packaged server exited during startup",
    );
    try {
      const response = await fetch(`${url}/health/ready`, {
        signal: AbortSignal.timeout(1000),
      });
      await response.arrayBuffer();
      if (response.ok) return;
    } catch {
      /* The socket may not be bound yet. */
    }
    await delay(50);
  }
  throw new Error("packaged server did not become ready");
}
async function call(method, path, body) {
  const response = await fetch(`${url}/v1/databases/${path}`, {
    method,
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  assert.equal(response.status, 200, `${method} ${path}`);
  return response.json();
}
let evidence;
try {
  // Read only the named file; archive paths never become extraction targets.
  writeFileSync(
    binary,
    execFileSync("tar", ["xOzf", archive, "durable-db"], {
      maxBuffer: 128 * 1024 * 1024,
    }),
  );
  chmodSync(binary, 0o700);
  assert.equal(run("--version"), `durable-db ${version}`);
  await start();
  token = run("token", "--key-file", join(temp, "data/admin.key"), "--admin");
  const create = {
    request_id: "create",
    limits: { max_keys: 10, max_bytes: 10000 },
  };
  assert.equal((await call("PUT", "source", create)).replayed, false);
  assert.equal((await call("PUT", "source", create)).replayed, true);
  const writes = [
    { key: "a", value: "b25l" },
    { key: "b", value: "dHdv" },
  ];
  const commit = { request_id: "write", expected_revision: 0, writes };
  assert.equal((await call("POST", "source/commit", commit)).revision, 1);
  assert.equal((await call("POST", "source/commit", commit)).replayed, true);
  const read = { keys: ["b", "missing"], scans: [{ prefix: "", limit: 10 }] };
  async function verify(database) {
    const result = await call("POST", `${database}/read`, read);
    assert.deepEqual(result.values, ["dHdv", null]);
    assert.deepEqual(result.scans[0].items, writes);
  }
  await verify("source");
  const tokenFile = join(temp, "token");
  const backup = join(temp, "backup.jsonl");
  writeFileSync(tokenFile, token, { mode: 0o600 });
  const exported = JSON.parse(
    run(
      "backup",
      "export",
      "--url",
      url,
      "--token-file",
      tokenFile,
      "--database",
      "source",
      "--output",
      backup,
    ),
  );
  assert.deepEqual(
    JSON.parse(run("backup", "inspect", "--input", backup)),
    exported,
  );
  const restored = JSON.parse(
    run(
      "backup",
      "restore",
      "--url",
      url,
      "--token-file",
      tokenFile,
      "--database",
      "restored",
      "--input",
      backup,
    ),
  );
  assert.equal(restored.verified_keys, 2);
  await verify("restored");
  await stop();
  await start();
  await verify("source");
  await verify("restored");
  assert.equal((await call("POST", "source/commit", commit)).replayed, true);
  evidence = {
    version,
    archive_sha256: digest,
    binary_sha256: sha256(readFileSync(binary)),
    empty_toolchain_path: true,
    protected_creation_and_replay: true,
    commit_and_exact_retry: true,
    point_and_ordered_reads: true,
    export_inspect_restore: true,
    kill_and_reopen: true,
    temporary_server_data_and_tokens_removed: true,
  };
} finally {
  await stop();
  rmSync(temp, { recursive: true, force: true });
}
console.log(JSON.stringify(evidence, null, 2));
