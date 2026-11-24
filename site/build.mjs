// Builds the public website into ../.site/. Run with: npm --prefix site run build
// Everything is rendered and checked in memory first; the output directory is only
// replaced when every page built and every local link resolves.
import { readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import MarkdownIt from 'markdown-it';

const root = new URL('../', import.meta.url);
const out = new URL('.site/', root);
const repository = 'https://github.com/ericflo/durable-db';

// The explicit public whitelist, in reading order: [page, label, tag, source, summary].
// Only these documents are published.
const pages = [
  ['deployment', 'Deployment', 'Operations', 'docs/deployment.md', 'Install the static binary and run it on a laptop, a bucket, or Kubernetes. Configuration, tenant limits, and metrics.'],
  ['api', 'HTTP API', 'Reference', 'docs/api.md', 'Transactions, checks, ordered scans, read consistency levels, idempotency keys, errors, and limits.'],
  ['clients', 'TypeScript SDK', 'Client', 'clients/javascript/README.md', 'Zero-dependency typed client with retries and revision tracking, plus the pi-durable agent storage adapter.'],
  ['architecture', 'Architecture', 'Protocol', 'docs/architecture.md', 'Bucket object layout, taking ownership, commits, reads, compaction, collection, and failure modes.'],
  ['performance', 'Performance', 'Benchmarks', 'docs/performance.md', 'Local NVMe, Backblaze B2, large-database, disk-cache, and Kubernetes measurements with commands to reproduce them.'],
  ['verification', 'Verification', 'Correctness', 'docs/verification.md', 'Claims, assumptions, the TLA+ model, 13 negative controls, chaos tests, all 47 findings, and remaining obligations.'],
  ['development', 'Development', 'Source', 'docs/development.md', 'Build from source with kache, run every check and soak script, repository layout, releases, and CI.'],
].map(([name, label, tag, source, summary]) => ({ name, label, tag, source, summary }));

// Files published unchanged at the site root: [published name, source].
const copies = [
  ['style.css', 'site/style.css'],
  ['site.js', 'site/site.js'],
  ['icon.svg', 'site/icon.svg'],
  ['install.sh', 'scripts/install.sh'],
  ['LICENSE', 'LICENSE'],
];

const problems = [];
const read = (path, encoding = 'utf8') =>
  readFile(new URL(path, root), encoding).catch(() => {
    throw new Error(`Cannot read ${path}, which the website publishes.`);
  });
// Replace every {{name}} of a template. An unknown name is an error, so none survives.
const fill = (template, values) =>
  template.replace(/\{\{(\w+)\}\}/g, (_match, name) => {
    if (!(name in values)) throw new Error(`Unknown template placeholder {{${name}}}`);
    return values[name];
  });

const cargo = (await read('Cargo.toml')).match(/^version = "([^"]+)"$/m);
if (!cargo) throw new Error('Cargo.toml has no package version.');
const version = `v${cargo[1]}`;

// A relative link is resolved against the Markdown file that contains it. A published
// document becomes its page, anything else under docs/ is an error, and the rest of
// the repository is linked on GitHub.
function resolve(href, page) {
  if (/^(?:[a-z][a-z0-9+.-]*:|#)/i.test(href)) return href;
  const target = new URL(href, new URL(page.source, 'file:///'));
  const path = decodeURIComponent(target.pathname.slice(1)).replace(/\/$/, '');
  const published = pages.find((other) => other.source === path);
  if (published) return `${published.name}.html${target.hash}`;
  if (path.startsWith('docs/')) problems.push(`${page.source} links to ${path}, which is not published.`);
  const kind = /\.[a-z0-9]+$/i.test(path) ? 'blob' : 'tree';
  return `${repository}/${kind}/main/${path}${target.hash}`;
}

const md = new MarkdownIt({ html: false });
const escape = md.utils.escapeHtml;

// Wrap Markdown tables so they preserve natural table layout while scrolling on narrow screens.
const defaultTableOpen = md.renderer.rules.table_open || ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options));
const defaultTableClose = md.renderer.rules.table_close || ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options));
md.renderer.rules.table_open = (tokens, idx, options, env, self) =>
  `<div class="table-wrap">${defaultTableOpen(tokens, idx, options, env, self)}`;
md.renderer.rules.table_close = (tokens, idx, options, env, self) =>
  `${defaultTableClose(tokens, idx, options, env, self)}</div>`;

// Give every heading an id and an anchor, collect the table of contents, rewrite links.
md.core.ruler.push('site', (state) => {
  const { page, toc, ids } = state.env;
  state.tokens.forEach((token, index) => {
    for (const child of token.children ?? [])
      if (child.type === 'link_open') child.attrSet('href', resolve(child.attrGet('href'), page));
    if (token.type !== 'heading_open') return;
    const inline = state.tokens[index + 1];
    const title = inline.children
      .filter((child) => child.type === 'text' || child.type === 'code_inline')
      .map((child) => child.content)
      .join('');
    if (token.tag === 'h1') return void (state.env.title ??= title);
    const base = title.toLowerCase().replace(/[^\p{L}\p{N}\s_-]/gu, '').trim().replace(/\s/g, '-');
    const seen = ids.get(base) ?? 0;
    ids.set(base, seen + 1);
    const id = seen ? `${base}-${seen}` : base;
    token.attrSet('id', id);
    if (token.tag === 'h2' || token.tag === 'h3') toc.push([id, title, token.tag]);
    const anchor = new state.Token('html_inline', '', 0);
    anchor.content = `<a class="anchor" href="#${id}" aria-label="Link to this section">#</a>`;
    inline.children.push(anchor);
  });
});

const layout = await read('site/layout.html');
const site = new Map(); // published name -> contents
// Wrap a page body in the shared layout and mark links to the page itself as current.
const publish = (name, title, description, pageClass, body) =>
  site.set(
    `${name}.html`,
    fill(layout, { title: escape(title), description: escape(description), version, pageClass, body })
      .replaceAll(`href="${name}.html"`, `href="${name}.html" aria-current="page"`),
  );

const sidebar = pages.map((page) => `<li><a href="${page.name}.html">${page.label}</a></li>`).join('');
for (const [index, page] of pages.entries()) {
  const env = { page, toc: [], ids: new Map() };
  const content = md.render(await read(page.source), env);
  if (!env.title) throw new Error(`${page.source} must begin with a "# Title" heading.`);
  const toc = env.toc.length < 2 ? '' : `<nav class="toc" aria-label="On this page"><p>On this page</p><ul>${env.toc
    .map(([id, text, tag]) => `<li class="${tag}"><a href="#${id}">${escape(text)}</a></li>`)
    .join('')}</ul></nav>`;
  const pager = [['previous', 'Previous', pages[index - 1]], ['next', 'Next', pages[index + 1]]]
    .map(([side, text, other]) => other ? `<a class="${side}" href="${other.name}.html"><span>${text}</span>${other.label}</a>` : '')
    .join('');
  publish(page.name, `${env.title} · durable-db`, page.summary, 'page-docs', `
<div class="docs wrap">
<details class="docs-nav" open><summary>Documentation</summary><nav aria-label="Documentation"><ul>${sidebar}</ul></nav></details>
<main id="main"><article><p class="doc-kicker">${escape(page.tag)} · ${version}</p>${content}</article><nav class="pager" aria-label="Previous and next page">${pager}</nav></main>
${toc}
</div>`);
}

const directory = pages
  .map((page) => `<li><a href="${page.name}.html"><span class="dir-tag">${escape(page.tag)}</span><strong>${page.label}</strong><span class="dir-summary">${page.summary}</span><span class="dir-arrow" aria-hidden="true">→</span></a></li>`)
  .join('');
publish(
  'index',
  'durable-db · A transactional database whose only state is a bucket',
  'One static Rust binary serving atomic transactions and ordered JSON scans over HTTP. All state lives as immutable objects in Backblaze B2, S3-compatible storage, or a local directory.',
  'page-home',
  fill(await read('site/home.html'), { version, directory }),
);
for (const [name, source] of copies) site.set(name, await read(source, null));
site.set('.nojekyll', '');

// Every local href and src must name a published file, and every fragment an id in it.
const idsOf = (file) => new Set([...String(site.get(file)).matchAll(/\sid="([^"]+)"/g)].map((match) => match[1]));
let links = 0;
for (const [name, html] of site) {
  if (!name.endsWith('.html')) continue;
  for (const [, raw] of html.matchAll(/\s(?:href|src)="([^"]*)"/g)) {
    const link = raw.replaceAll('&amp;', '&');
    if (/^[a-z][a-z0-9+.-]*:/i.test(link)) continue;
    const [path, fragment] = link.split('#');
    const file = path || name;
    if (!site.has(file)) problems.push(`${name} links to ${link}, which is not published.`);
    else if (fragment && !idsOf(file).has(decodeURIComponent(fragment)))
      problems.push(`${name} links to ${link}, but ${file} has no such heading.`);
    links++;
  }
}
if (problems.length) {
  console.error(`The website was not built:\n- ${[...new Set(problems)].join('\n- ')}`);
  process.exit(1);
}

await rm(out, { recursive: true, force: true });
await mkdir(out, { recursive: true });
for (const [name, contents] of site) await writeFile(new URL(name, out), contents);
console.log(`Built ${pages.length + 1} pages for ${version} into .site/ and checked ${links} local links.`);
