import { readFile, writeFile, mkdir, copyFile, rm } from 'node:fs/promises';
import MarkdownIt from 'markdown-it';
const out = new URL('../.site/', import.meta.url);
const docs = [
  'architecture',
  'b2',
  'api',
  'recovery',
  'deployment',
  'observability',
  'security',
  'verification',
  'verification-results',
  'clients',
  'web-app',
  'development',
  'production-readiness',
];
await rm(out, { recursive: true, force: true });
await mkdir(out, { recursive: true });
const md = new MarkdownIt({ html: false, linkify: false, typographer: true });
md.renderer.rules.heading_open = (tokens, index, options, env, self) => {
  const title = tokens[index + 1].children
    .filter((t) => t.type === 'text' || t.type === 'code_inline')
    .map((t) => t.content)
    .join('');
  const base = title
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s_-]/gu, '')
    .trim()
    .replace(/\s/g, '-');
  const seen = env.headingIds.get(base) || 0;
  env.headingIds.set(base, seen + 1);
  tokens[index].attrSet('id', seen ? `${base}-${seen}` : base);
  return self.renderToken(tokens, index, options);
};
const nav = `<a class="wordmark" href="index.html"><span class="mark" aria-hidden="true">▤</span> durable-db</a><nav aria-label="Main"><a href="architecture.html">Architecture</a><a href="api.html">API</a><a href="verification.html">Verification</a><a class="nav-button" href="development.html">Build it ↗</a></nav>`;
function layout(title, body, home = false) {
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="description" content="An experimental Rust transactional database with authoritative B2 or S3 storage and disposable compute."><meta name="color-scheme" content="light"><title>${title} · durable-db</title><link rel="icon" href="icon.svg" type="image/svg+xml"><link rel="stylesheet" href="style.css"></head><body><header>${nav}</header>${body}<footer><a class="wordmark" href="index.html">durable-db</a><p>Apache-2.0 · Built in Rust · Verified within explicit bounds</p><a href="production-readiness.html">Experimental. Follow the work ↗</a></footer>${home ? '<script src="demo.js" defer></script>' : ''}</body></html>`;
}
const designDocs = [
  'design-owned-partitions',
  'design-signing-rotation',
  'design-upload-admission',
  'design-budgetless-collection',
  'proof-publication',
];
for (const doc of [...docs, ...designDocs]) {
  const source = await readFile(new URL(`../docs/${doc}.md`, import.meta.url), 'utf8');
  const title = source.split('\n')[0].replace(/^# /, '');
  const content = md
    .render(source, { headingIds: new Map() })
    .replace(/href="([a-z0-9-]+)\.md(#[^"]*)?"/g, (_match, name, fragment = '') => {
      if (![...docs, ...designDocs].includes(name))
        throw new Error(`Unpublished documentation target: ${name}`);
      return `href="${name}.html${fragment}"`;
    });
  const sidebar = docs
    .map(
      (d) =>
        `<a ${doc === d ? 'aria-current="page"' : ''} href="${d}.html">${d.replaceAll('-', ' ')}</a>`,
    )
    .join('');
  await writeFile(
    new URL(`${doc}.html`, out),
    layout(
      title,
      `<main class="docs"><aside aria-label="Documentation">${sidebar}</aside><article>${content}</article></main>`,
    ),
  );
}
// Preserve the old public URL; release work has one maintained source.
await writeFile(new URL('roadmap.html', out), '<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Release plan · durable-db</title><meta http-equiv="refresh" content="0;url=production-readiness.html"><link rel="canonical" href="production-readiness.html"></head><body><a href="production-readiness.html">Production readiness and release plan</a></body></html>');
const home = await readFile(new URL('home.html', import.meta.url), 'utf8');
await writeFile(
  new URL('index.html', out),
  layout('Storage that stays. Compute that scales.', home, true),
);
for (const file of ['style.css', 'demo.js', 'icon.svg'])
  await copyFile(new URL(file, import.meta.url), new URL(file, out));
await mkdir(new URL('evidence/', out), { recursive: true });
for (const file of [
  'transaction-validation-2026-11-09.json',
  'late-control-candidate-2026-11-09.json',
  'placement-requests-candidate-2026-11-09.json',
  'placement-takeover-candidate-2026-11-09.json',
  'control-session-2026-11-09.json',
  'root-requests-candidate-2026-11-09.json',
  'root-requests-proof-2026-11-09.json',
  'ownership-timing-2026-11-09.json',
  'request-budget-2026-11-09.json',
  'root-controller-2026-11-09.json',
  'root-registration-2026-11-09.json',
  'root-catalog-2026-11-09.json',
  'catalog-replay-2026-11-09.json',
  'transaction-recovery-2026-11-09.json',
  'slatedb-contract-2026-11-09.json',
  'storage-ranges-2026-11-09.json',
  'native-tables-2026-11-09.json',
  'table-merge-2026-11-09.json',
  'native-recovery-2026-11-09.json',
  'native-checkpoints-2026-11-09.json',
  'checkpoint-reads-2026-11-09.json',
  'checkpoint-overlay-2026-11-09.json',
  'checkpoint-reuse-2026-11-10.json',
  'native-http-hosts-2026-11-10.json',
  'vm-clock-2026-11-10.json',
  'index-read-cost-2026-11-09.json',
  'partition-journal-candidate-2026-11-09.json',
  'partition-control-candidate-2026-11-09.json',
  'provider-register-2026-11-09.json',
  'root-journal-candidate-2026-11-09.json',
  'grant-bounds-2026-11-09.json',
  'root-control-candidate-2026-11-09.json',
  'journal-2026-11-09.json',
  'delegated-grant-candidate-2026-11-09.json',
  'owned-wal-candidate-2026-11-09.json',
  'provider-floor-2026-11-09.json',
  'wal-handoff-candidate-2026-11-09.json',
  'physical-memory-hour-2026-11-09.json',
  'physical-memory-hour-2026-11-09.history.jsonl',
  'serving-authority-candidate-2026-11-09.json',
  'json-sdk-2026-11-09.json',
  'client-web-app-2026-11-09.json',
  'physical-memory-preflight-2026-11-09.json',
  'physical-memory-preflight-2026-11-09.history.jsonl',
  'pi-harness-2026-11-09.json',
  'directory-capacity-2026-11-09.json',
  'physical-b2-2026-11-09.json',
  'physical-b2-2026-11-09.history.jsonl',
  'publication-proof-2026-11-09.json',
  'kubernetes-budget-b2-2026-11-09.json',
  'kubernetes-budget-b2-2026-11-09.history.jsonl',
  'logical-backup-2026-11-09.json',
  'kubernetes-budget-minio-2026-11-09.json',
  'kubernetes-budget-minio-2026-11-09.history.jsonl',
  'global-collection-2026-11-09.json',
  'upload-budget-2026-11-09.json',
  'kubernetes-b2-2026-11-09.json',
  'kubernetes-b2-2026-11-09.history.jsonl',
  'kubernetes-b2-calibration-incomplete-2026-11-09.json',
  'kubernetes-oom-minio-2026-11-09.json',
  'kubernetes-oom-minio-2026-11-09.history.jsonl',
  'kubernetes-oom-calibration-incomplete-2026-11-09.json',
  'kubernetes-hpa-minio-2026-11-09.json',
  'kubernetes-hpa-minio-2026-11-09.history.jsonl',
  'kubernetes-hpa-first-incomplete-2026-11-09.json',
  'signing-policy-minio-2026-11-09.json',
  'pi-indexed-b2-2026-11-09.json',
  'b2-contention-2026-11-08.json',
  'b2-isolation-2026-11-08.json',
  'b2-recovery-2026-11-08.json',
  'b2-rebase-2026-11-08.json',
  'b2-grouped-2026-11-08.json',
  'b2-profile-isolation-2026-11-08.json',
  'b2-profile-metrics-2026-11-08.json',
  'mixed-combining-minio-2026-11-08.json',
  'b2-combining-2026-11-08.json',
  'b2-combining-metrics-2026-11-08.json',
  'mixed-combining-b2-2026-11-08.json',
  'mixed-combining-b2-failed-2026-11-08.json',
  'mixed-combining-b2-admin-limit-2026-11-08.json',
  'mixed-combining-b2-partial-recovery-2026-11-08.json',
  'policy-upgrade-minio-2026-11-08.json',
  'policy-upgrade-b2-2026-11-08.json',
  'protected-creation-minio-2026-11-08.json',
  'protected-creation-b2-2026-11-08.json',
]) {
  await copyFile(
    new URL(`../docs/evidence/${file}`, import.meta.url),
    new URL(`evidence/${file}`, out),
  );
}
await copyFile(new URL('../scripts/install.sh', import.meta.url), new URL('install.sh', out));
await mkdir(new URL('prometheus/', out), { recursive: true });
for (const file of ['alerts.yml', 'alerts.test.yml']) {
  await copyFile(
    new URL(`../deploy/prometheus/${file}`, import.meta.url),
    new URL(`prometheus/${file}`, out),
  );
}
await writeFile(new URL('.nojekyll', out), '');
// Explicit whitelist: integration plans and private source never enter the public site.
// Validate the rendered publication, including evidence downloads and heading links.
// Preserve published URLs while removing superseded source documents.
const redirects = { 'architecture-reassessment': 'design-owned-partitions' };
for (const [old, target] of Object.entries(redirects)) {
  await writeFile(
    new URL(`${old}.html`, out),
    `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Design moved · durable-db</title><meta http-equiv="refresh" content="0;url=${target}.html"><link rel="canonical" href="${target}.html"></head><body><p>The current <a href="${target}.html">storage design</a> includes the architecture decision and implementation status.</p></body></html>`,
  );
}
const publishedPages = ['index', ...docs, ...designDocs, ...Object.keys(redirects)];
const rendered = new Map(
  await Promise.all(
    publishedPages.map(async (name) => [
      `${name}.html`,
      await readFile(new URL(`${name}.html`, out), 'utf8'),
    ]),
  ),
);
let checkedLinks = 0;
for (const [name, html] of rendered) {
  for (const match of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
    const href = match[1].replaceAll('&amp;', '&');
    if (/^(?:https?:|mailto:|data:|\/\/)/.test(href)) continue;
    const target = new URL(href, new URL(name, out));
    if (!target.href.startsWith(out.href))
      throw new Error(`Link escapes the public site: ${name}: ${href}`);
    const fragment = decodeURIComponent(target.hash.slice(1));
    target.hash = '';
    target.search = '';
    const body = await readFile(target, 'utf8').catch(() => {
      throw new Error(`Missing public link: ${name}: ${href}`);
    });
    if (
      fragment &&
      target.pathname.endsWith('.html') &&
      ![...body.matchAll(/id="([^"]+)"/g)].some((item) => item[1] === fragment)
    )
      throw new Error(`Missing public heading: ${name}: ${href}`);
    checkedLinks++;
  }
}
console.log(`Built ${docs.length + designDocs.length + 1} public pages into .site/`);
console.log(`Validated ${checkedLinks} local page, asset, evidence, and heading links.`);
