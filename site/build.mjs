import {readFile,writeFile,mkdir,copyFile} from 'node:fs/promises';
import MarkdownIt from 'markdown-it';
const out=new URL('../.site/',import.meta.url);
const docs=['architecture','b2','api','deployment','security','verification','verification-results','clients','development','roadmap'];
await mkdir(out,{recursive:true});
const md=new MarkdownIt({html:false,linkify:false,typographer:true});
const nav=`<a class="wordmark" href="index.html"><span class="mark" aria-hidden="true">▤</span> durable-db</a><nav aria-label="Main"><a href="architecture.html">Architecture</a><a href="api.html">API</a><a href="verification.html">Verification</a><a class="nav-button" href="development.html">Build it ↗</a></nav>`;
function layout(title,body,home=false){return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="description" content="An experimental Rust transactional database with authoritative B2 or S3 storage and disposable compute."><meta name="color-scheme" content="light"><title>${title} · durable-db</title><link rel="icon" href="icon.svg" type="image/svg+xml"><link rel="stylesheet" href="style.css"></head><body><header>${nav}</header>${body}<footer><a class="wordmark" href="index.html">durable-db</a><p>Apache-2.0 · Built in Rust · Verified within explicit bounds</p><a href="roadmap.html">Experimental. Follow the work ↗</a></footer>${home?'<script src="demo.js" defer></script>':''}</body></html>`;}
for(const doc of docs){
 const source=await readFile(new URL(`../docs/${doc}.md`,import.meta.url),'utf8');
 const title=source.split('\n')[0].replace(/^# /,'');
 const content=md.render(source).replace(/href="([a-z-]+)\.md(#[^"]*)?"/g,'href="$1.html$2"');
 const sidebar=docs.map(d=>`<a ${doc===d?'aria-current="page"':''} href="${d}.html">${d.replaceAll('-',' ')}</a>`).join('');
 await writeFile(new URL(`${doc}.html`,out),layout(title,`<main class="docs"><aside aria-label="Documentation">${sidebar}</aside><article>${content}</article></main>`));
}
const home=await readFile(new URL('home.html',import.meta.url),'utf8');
await writeFile(new URL('index.html',out),layout('Storage that stays. Compute that scales.',home,true));
for(const file of ['style.css','demo.js','icon.svg']) await copyFile(new URL(file,import.meta.url),new URL(file,out));
await writeFile(new URL('.nojekyll',out),'');
// Explicit whitelist: integration plans and private source never enter the public site.
console.log(`Built ${docs.length+1} public pages into .site/`);
