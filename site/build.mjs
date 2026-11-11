import { readFile, writeFile, mkdir, copyFile, rm } from 'node:fs/promises';
import MarkdownIt from 'markdown-it';
const out = new URL('../.site/', import.meta.url);
// The explicit public whitelist, in navigation order: [page, short label, summary].
// Integration plans and other unlisted Markdown never enter the public site.
const sections = [
  ['Start', [
    ['deployment', 'Deployment', 'Install, run locally, then deploy on S3, B2 or Kubernetes.'],
    ['api', 'HTTP API', 'Endpoints, snapshots, commits, retries, errors and limits.'],
    ['clients', 'Clients', 'The TypeScript SDK, the wire client and pi-durable adapters.'],
    ['web-app', 'Web app example', 'A task board built on the TypeScript SDK.'],
  ]],
  ['Understand', [
    ['architecture', 'Architecture', 'Authority, the commit protocol and fenced collection.'],
    ['b2', 'B2 protocol', 'How Backblaze B2 provides conditional publication.'],
    ['security', 'Security', 'Trust boundaries, credentials and resource isolation.'],
  ]],
  ['Operate', [
    ['recovery', 'Backup and restore', 'Consistent export, resumable restore and format upgrades.'],
    ['observability', 'Observability', 'Prometheus metrics, alerts and incident response.'],
    ['development', 'Development', 'Building from source, focused checks and CI.'],
  ]],
  ['Verify', [
    ['production-readiness', 'Production readiness', 'Release gates and the evidence needed to close them.'],
    ['verification', 'Verification', 'Models, assumptions, checked bounds and open obligations.'],
    ['verification-results', 'Verification results', 'Dated experiments and release qualification.'],
    ['proof-publication', 'Publication proof', 'An unbounded TLAPS proof of abstract page reachability.'],
  ]],
  ['Design notes', [
    ['design-owned-partitions', 'Owned partitions', 'The native serving and durable-log replacement.'],
    ['design-upload-admission', 'Upload admission', 'Durable, cumulative limits on immutable uploads.'],
    ['design-budgetless-collection', 'Budgetless collection', 'Collecting data pages without upload credit.'],
    ['design-signing-rotation', 'Signing key rotation', 'Overlapping signing keys and rolling rotation.'],
  ]],
];
const pages = sections.flatMap(([section, entries]) => entries.map(([name, label, summary]) => ({ name, label, summary, section })));
const docs = pages.map((page) => page.name);
const escape = (text) => MarkdownIt().utils.escapeHtml(text);
// The homepage advertises exactly the release the public installer installs.
const version = (await readFile(new URL('../scripts/install.sh', import.meta.url), 'utf8')).match(/^version=\$\{1:-(v[0-9][0-9A-Za-z.-]*)\}$/m)?.[1];
if (!version) throw new Error('Installer default release not found');
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
  const id = seen ? `${base}-${seen}` : base;
  tokens[index].attrSet('id', id);
  if (tokens[index].tag === 'h2') env.toc.push([id, title]);
  return self.renderToken(tokens, index, options);
};
md.renderer.rules.heading_close = (tokens, index, options, env, self) => {
  const id = tokens[index - 2].attrGet('id');
  const anchor = tokens[index].tag === 'h1' ? '' : `<a class="anchor" href="#${id}" aria-label="Link to this section">#</a>`;
  return anchor + self.renderToken(tokens, index, options);
};
const navigation = [['index.html#docs', 'Docs'], ['api.html', 'API'], ['architecture.html', 'Architecture'], ['verification.html', 'Verification'], ['production-readiness.html', 'Status']];
function layout({ title, description, body, current = '' }) {
  const links = navigation.map(([href, label]) => `<a href="${href}"${href === `${current}.html` ? ' aria-current="page"' : ''}>${label}</a>`).join('');
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${title}</title><meta name="description" content="${escape(description)}"><meta property="og:title" content="${title}"><meta property="og:description" content="${escape(description)}"><meta name="color-scheme" content="light dark"><link rel="icon" href="icon.svg" type="image/svg+xml"><link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400..700&family=IBM+Plex+Mono:wght@400;500&display=swap"><link rel="stylesheet" href="style.css"><script>document.documentElement.classList.add('js')</script><script src="site.js" defer></script></head><body><a class="skip" href="#main">Skip to content</a><header class="site-header"><div class="wrap"><a class="wordmark" href="index.html"><img src="icon.svg" alt="" width="28" height="28">durable-db</a><a class="release" href="production-readiness.html">${version} · experimental</a><nav aria-label="Main">${links}</nav><a class="button button-small" href="index.html#quickstart">Get started</a></div></header>${body}<footer class="site-footer"><div class="wrap"><div><a class="wordmark" href="index.html"><img src="icon.svg" alt="" width="24" height="24">durable-db</a><p>An experimental transactional key-value database in one Rust binary. Apache-2.0.</p></div><nav aria-label="Footer"><a href="index.html#quickstart">Quick start</a><a href="api.html">HTTP API</a><a href="clients.html">TypeScript SDK</a><a href="verification-results.html">Evidence</a><a href="production-readiness.html">Release gates</a><a href="install.sh">install.sh</a></nav></div></footer></body></html>`;
}
const tocs = new Map();
for (const [index, page] of pages.entries()) {
  const source = await readFile(new URL(`../docs/${page.name}.md`, import.meta.url), 'utf8');
  const title = source.split('\n')[0].replace(/^# /, '');
  const env = { headingIds: new Map(), toc: [] };
  tocs.set(page.name, env.toc);
  const content = md
    .render(source, env)
    .replace(/href="([a-z0-9-]+)\.md(#[^"]*)?"/g, (_match, name, fragment = '') => {
      if (!docs.includes(name))
        throw new Error(`Unpublished documentation target: ${name}`);
      return `href="${name}.html${fragment}"`;
    });
  const sidebar = sections
    .map(([section, entries]) => `<p>${section}</p><ul>${entries.map(([name, label]) => `<li><a href="${name}.html"${name === page.name ? ' aria-current="page"' : ''}>${label}</a></li>`).join('')}</ul>`)
    .join('');
  const toc = env.toc.length > 1
    ? `<nav class="toc" aria-label="On this page"><p>On this page</p><ol>${env.toc.map(([id, text]) => `<li><a href="#${id}">${escape(text)}</a></li>`).join('')}</ol></nav>`
    : '';
  const pager = [pages[index - 1], pages[index + 1]]
    .map((other, side) => other ? `<a class="${side ? 'next' : 'previous'}" href="${other.name}.html"><span>${side ? 'Next' : 'Previous'}</span>${other.label}</a>` : '<span></span>')
    .join('');
  await writeFile(
    new URL(`${page.name}.html`, out),
    layout({
      title: `${escape(title)} · durable-db`,
      description: page.summary,
      current: page.name,
      body: `<main id="main" class="docs wrap"><details class="docs-nav" open><summary>Documentation</summary><nav aria-label="Documentation">${sidebar}</nav></details><article><p class="eyebrow">${page.section}</p>${content}<nav class="pager" aria-label="Pagination">${pager}</nav></article>${toc}</main>`,
    }),
  );
}
// Preserve the old public URL; release work has one maintained source.
await writeFile(new URL('roadmap.html', out), '<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Release plan · durable-db</title><meta http-equiv="refresh" content="0;url=production-readiness.html"><link rel="canonical" href="production-readiness.html"></head><body><a href="production-readiness.html">Production readiness and release plan</a></body></html>');
// Recent milestones come from the dated verification record, newest first.
const milestones = tocs.get('verification-results').slice(0, 5).map(([id, heading]) => {
  const [, title, dated, dashed] = heading.match(/^(.*?)(?:\s+\((\d{4}-\d{2}-\d{2})\)|\s+—\s+(\d{4}-\d{2}-\d{2}))?$/);
  const date = dated || dashed || '';
  return `<li><a href="verification-results.html#${id}"><span>${escape(title)}</span><time>${date}</time></a></li>`;
});
const directory = sections
  .map(([section, entries]) => `<section><h3>${section}</h3><ul>${entries.map(([name, label, summary]) => `<li><a href="${name}.html"><strong>${label}</strong><span>${summary}</span></a></li>`).join('')}</ul></section>`)
  .join('');
const home = (await readFile(new URL('home.html', import.meta.url), 'utf8'))
  .replaceAll('{{version}}', version)
  .replace('{{milestones}}', milestones.join(''))
  .replace('{{directory}}', directory);
if (home.includes('{{')) throw new Error('Unreplaced homepage placeholder');
await writeFile(
  new URL('index.html', out),
  layout({
    title: 'durable-db · Transactional key-value storage that lives in your bucket',
    description: 'An experimental transactional, ordered key-value database in one Rust binary. S3 or B2 holds the data; API servers are replaceable.',
    current: 'index',
    body: home,
  }),
);
for (const file of ['style.css', 'site.js', 'icon.svg'])
  await copyFile(new URL(file, import.meta.url), new URL(file, out));
await mkdir(new URL('evidence/', out), { recursive: true });
for (const file of [
  'release-0.2.0-2026-11-10.json',
  'release-0.3.0-2026-11-10.json',
  'inline-checkpoints-2026-11-10.json',
  'native-table-collection-2026-11-10.json',
  'checkpoint-index-reads-2026-11-10.json',
  'tenant-checkpoint-admission-2026-11-10.json',
  'retained-receipt-admission-2026-11-10.json',
  'shared-table-admission-2026-11-10.json',
  'native-local-cli-2026-11-10.json',
  'index-copy-admission-2026-11-10.json',
  'row-overhead-admission-2026-11-10.json',
  'checkpoint-retention-2026-11-10.json',
  'retained-row-reblocking-2026-11-10.json',
  'manifest-boundaries-2026-11-10.json',
  'manifest-path-admission-2026-11-10.json',
  'manifest-growth-admission-2026-11-10.json',
  'checkpoint-retry-pacing-2026-11-10.json',
  'retained-export-2026-11-10.json',
  'maintenance-funding-2026-11-10.json',
  'metadata-retry-2026-11-10.json',
  'catalog-admission-2026-11-10.json',
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
  'native-http-budgets-2026-11-10.json',
  'native-catalog-collection-2026-11-10.json',
  'native-journal-collection-2026-11-10.json',
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
  'revocable-owner-2026-11-11.json',
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
const publishedPages = ['index', ...docs, ...Object.keys(redirects)];
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
console.log(`Built ${docs.length + 1} public pages into .site/`);
console.log(`Validated ${checkedLinks} local page, asset, evidence, and heading links.`);
