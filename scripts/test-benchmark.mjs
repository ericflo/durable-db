// Verify that the measurement tool reports exhausted ambiguous writes as failures,
// retains their exact retry identity, and still checks acknowledged data afterward.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {spawn} from 'node:child_process';
import {mkdtemp, writeFile, readFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {once} from 'node:events';

const root=await mkdtemp(join(tmpdir(),'durable-db-benchmark-test-'));
const databases=new Map();
let retries=0, changedRetry=false, firstBody;
const server=createServer(async (req,res)=>{
  const chunks=[]; for await (const chunk of req) chunks.push(chunk);
  const text=Buffer.concat(chunks).toString();
  const body=text ? JSON.parse(text) : {};
  res.setHeader('content-type','application/json');
  if (req.url==='/v1/tokens') return res.end(JSON.stringify({token:'fixture-only'}));
  const [,name,action]=req.url.match(/^\/v1\/databases\/([^/]+)(?:\/(.+))?$/);
  if (!action) { databases.set(name,{revision:0,value:null}); return res.end('{}'); }
  const db=databases.get(name);
  if (action==='read') return res.end(JSON.stringify({revision:db.revision,values:[db.value]}));
  if (name.endsWith('000001') && body.expected_revision===1) {
    // Apply once, but make every acknowledgement unavailable until the client gives up.
    firstBody??=text;
    if (text!==firstBody) changedRetry=true;
    db.revision=2; db.value=body.writes[0].value; retries++;
    res.statusCode=503; return res.end('{}');
  }
  db.revision++; db.value=body.writes[0].value;
  res.end(JSON.stringify({revision:db.revision,replayed:false}));
});
try {
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  const token=join(root,'token'), output=join(root,'result.json');
  await writeFile(token,'fixture-only',{mode:0o600});
  const child=spawn(process.execPath,[new URL('./benchmark-http.mjs',import.meta.url).pathname,
    '--urls',`http://127.0.0.1:${server.address().port}`,'--token-file',token,'--output',output,
    '--databases','2','--workers','2','--operations','2'],{stdio:'ignore',timeout:60_000});
  const [code]=await once(child,'exit');
  assert.equal(code,1);
  assert.equal(retries,12); assert.equal(changedRetry,false);
  const result=JSON.parse(await readFile(output,'utf8'));
  assert.equal(result.complete,false); assert.equal(result.failure,'phase_failed');
  assert.equal(result.phases.length,1);
  const p=result.phases[0];
  assert.equal(p.planned,4); assert.equal(p.started,3);
  assert.equal(p.succeeded,2); assert.equal(p.failed,1); assert.equal(p.not_started,1);
  assert.equal(p.all_started_latency.samples,3); assert.equal(p.successful_latency.samples,2);
  assert.equal(p.attempt_statuses['503'],12);
  assert.equal(result.verification.checked,2); assert.equal(result.verification.failed,0);
  assert.equal(result.verification.uncertain_write_observed,1);
  console.log('Benchmark exhausted-retry accounting, identity, and recovery verification passed');
} finally {
  server.closeAllConnections(); server.close();
  await rm(root,{recursive:true,force:true});
}
