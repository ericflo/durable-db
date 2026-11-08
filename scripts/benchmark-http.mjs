#!/usr/bin/env node
// Disposable-test workload. Creates databases and leaves them for caller cleanup.
// Closed-loop latency baseline, NOT an open-loop overload or long-running soak test.
import {readFile, writeFile, rename} from 'node:fs/promises';
import {parseArgs} from 'node:util';
import {randomUUID, createHash} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {setTimeout as sleep} from 'node:timers/promises';

const {values: args} = parseArgs({options: {
  urls: {type:'string', default:'http://127.0.0.1:7654'},
  'token-file': {type:'string'}, output: {type:'string', default:'benchmark.json'},
  databases: {type:'string', default:'70'}, workers: {type:'string', default:'1,4'},
  operations: {type:'string', default:'20'}, 'value-bytes': {type:'string', default:'1024'},
  binary: {type:'string'}, context: {type:'string', default:'Unspecified deployment'},
}});
function integer(value, min, max) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < min || n > max) throw Error('Invalid numeric argument');
  return n;
}
const count = integer(args.databases, 1, 100_000);
const workers = args.workers.split(',').map(n => integer(n, 1, Math.min(count,128)));
const operations = integer(args.operations, 1, 10_000);
const valueBytes = integer(args['value-bytes'], 32, 64*1024);
if (workers.reduce((s,n)=>s+n,0)*operations*2 > 1_000_000) throw Error('Too many samples');
if (!args['token-file']) throw Error('--token-file is required');
const admin = (await readFile(args['token-file'],'utf8')).trim();
const urls = args.urls.split(',').map(raw => {
  const u = new URL(raw);
  if (!['http:','https:'].includes(u.protocol) || u.username || u.password || u.search || u.hash || u.pathname!=='/') {
    throw Error('URLs must be HTTP(S) origins without credentials');
  }
  return u.origin;
});
const report = {
  schema:1, started_at:new Date().toISOString(), context:args.context,
  binary_sha256:args.binary ? createHash('sha256').update(await readFile(args.binary)).digest('hex') : null,
  configuration:{databases:count,workers,operations_per_worker:operations,value_bytes:valueBytes,
    endpoints:urls.length,max_attempts:12,operation_deadline_seconds:120,load_model:'closed-loop'},
  limitations:['Small samples are descriptive, not an SLO guarantee.',
    'Closed-loop load does not measure queue growth at a fixed arrival rate.',
    'Provider request counts, billed storage, and cost are not measured.',
    'Setup is excluded; retries, backoff, and failed operations are included in each measured phase.'],
  setup:{databases_created:0,attempt_statuses:{}},phases:[],verification:{checked:0,failed:0},complete:false,
};
async function save() {
  await writeFile(args.output+'.tmp', JSON.stringify(report,null,2)+'\n');
  await rename(args.output+'.tmp', args.output);
}
const bump = (counts,key) => counts[key] = (counts[key]??0)+1;
function distribution(samples) {
  const a = [...samples].sort((a,b)=>a-b);
  const at = p => a.length ? Number(a[Math.max(0,Math.ceil(p*a.length)-1)].toFixed(3)) : null;
  return {samples:a.length,p50_ms:at(.5),p95_ms:at(.95),p99_ms:at(.99),max_ms:at(1)};
}
async function request(url, path, token, body, counts, method='POST') {
  const started = performance.now();
  for (let attempt=0; attempt<12; attempt++) {
    const remaining = 120_000-(performance.now()-started);
    if (remaining<=0) break;
    let status, result;
    try {
      const response = await fetch(url+path, {method,
        headers:{authorization:'Bearer '+token,'content-type':'application/json'},
        ...(body===undefined ? {} : {body:JSON.stringify(body)}),
        signal:AbortSignal.timeout(Math.max(1,Math.ceil(Math.min(35_000,remaining)))),
      });
      status=response.status;
      bump(counts,String(status));
      if (response.ok) {
        result=await response.json();
        return result;
      }
      await response.body?.cancel();
    } catch {
      bump(counts,'transport_or_decode_error');
    }
    // A single worker owns each database; 409 is publication contention here.
    // Ambiguous writes retry the EXACT same revision, ID, and body.
    if (status && ![409,429,503].includes(status)) throw Error('nonretryable_http_'+status);
    if (attempt<11) await sleep(Math.min(1000,50*2**attempt)*(0.5+Math.random()/2));
  }
  throw Error('retry_budget_exhausted');
}
function payload(worker,sequence) {
  const b=Buffer.alloc(valueBytes,120);
  b.write(`${worker}:${sequence}:`);
  return b.toString('base64');
}
const databases = [];
try {
  const prefix = 'bench-'+randomUUID();
  for (let n=0;n<count;n++) {
    const db={name:`${prefix}-${String(n).padStart(6,'0')}`,revision:0,value:payload(n,0)};
    await request(urls[0],'/v1/databases/'+db.name,admin,{max_bytes:1048576,max_keys:1000},report.setup.attempt_statuses);
    const {token}=await request(urls[0],'/v1/tokens',admin,{database:db.name,write:true,ttl_seconds:86400},report.setup.attempt_statuses);
    db.token=token;
    const seed=await request(urls[0],`/v1/databases/${db.name}/commit`,token,
      {expected_revision:0,request_id:randomUUID(),writes:[{key:'value',value:db.value}]},report.setup.attempt_statuses);
    db.revision=seed.revision;
    databases.push(db);
    report.setup.databases_created++;
    if ((n+1)%10===0) { console.log(JSON.stringify({setup_databases:n+1})); await save(); }
  }
  await save();
  for (const concurrency of workers) {
    for (const mode of ['write','read']) {
      const phase={mode,workers:concurrency,planned:concurrency*operations,started:0,succeeded:0,failed:0,
        attempt_statuses:{},failure_reasons:{},samples:[]};
      const begin=performance.now();
      await Promise.all(Array.from({length:concurrency},async (_,worker)=>{
        const db=databases[worker];
        for (let n=0;n<operations;n++) {
          const start=performance.now();
          phase.started++;
          let ok=false;
          try {
            const url=urls[(worker+n)%urls.length];
            if (mode==='write') {
              const value=payload(worker,db.revision);
              const r=await request(url,`/v1/databases/${db.name}/commit`,db.token,
                {expected_revision:db.revision,request_id:randomUUID(),writes:[{key:'value',value}]},phase.attempt_statuses);
              if (r.revision!==db.revision+1) throw Error('incorrect_write_revision');
              db.revision=r.revision; db.value=value;
            } else {
              const r=await request(url,`/v1/databases/${db.name}/read`,db.token,{keys:['value']},phase.attempt_statuses);
              if (r.revision!==db.revision || r.values[0]!==db.value) throw Error('incorrect_read');
            }
            phase.succeeded++; ok=true;
          } catch (error) {
            phase.failed++; bump(phase.failure_reasons,error.message);
          }
          phase.samples.push({worker,ok,elapsed_ms:Number((performance.now()-start).toFixed(3))});
          // An exhausted ambiguous write must not be followed by a fresh write ID.
          if (!ok) break;
        }
      }));
      phase.elapsed_seconds=Number(((performance.now()-begin)/1000).toFixed(6));
      phase.not_started=phase.planned-phase.started;
      phase.successful_operations_per_second=Number((phase.succeeded/phase.elapsed_seconds).toFixed(3));
      phase.all_started_latency=distribution(phase.samples.map(s=>s.elapsed_ms));
      phase.successful_latency=distribution(phase.samples.filter(s=>s.ok).map(s=>s.elapsed_ms));
      report.phases.push(phase);
      const {samples,...summary}=phase;
      console.log(JSON.stringify(summary)); await save();
      if (phase.failed) throw Error('phase_failed');
    }
  }
  // Check every seeded database, including the idle population. Read through all endpoints.
  for (let n=0;n<databases.length;n++) {
    const db=databases[n];
    const r=await request(urls[n%urls.length],`/v1/databases/${db.name}/read`,db.token,{keys:['value']},{});
    report.verification.checked++;
    if (r.revision!==db.revision || r.values[0]!==db.value) report.verification.failed++;
  }
  if (report.verification.failed) throw Error('final_verification_failed');
  report.complete=true;
} catch (error) {
  // Messages are generated above; no HTTP bodies, credentials, or database payloads are logged.
  report.failure=error.message;
  process.exitCode=1;
} finally {
  report.finished_at=new Date().toISOString();
  await save();
  console.log(JSON.stringify({complete:report.complete,report:args.output,verification:report.verification}));
}
