// Synthetic sequential append workload for the isolated multi-node qualifier.
// No tokens, keys or stored values are written to logs or the report.
import {readFileSync} from 'node:fs';
import {createServer} from 'node:http';
import {randomUUID, createHash, timingSafeEqual} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {join} from 'node:path';
import {AsyncLocalStorage} from 'node:async_hooks';
import {subscribe} from 'node:diagnostics_channel';
import {requestJson,DbError} from '../clients/javascript/transport.mjs';

// Observe the SDK transport's actual pooled sockets without altering requests. Only
// numeric identities enter the report; never headers, paths or socket addresses.
const activeAttempt=new AsyncLocalStorage(), sockets=new WeakMap();
let nextSocket=0;
subscribe('http.client.request.created',({request})=>{
  const attempt=activeAttempt.getStore();if(!attempt)return;
  request.once('socket',socket=>{
    if(!sockets.has(socket))sockets.set(socket,++nextSocket);
    attempt.connection=sockets.get(socket);
  });
});
const origin=process.env.DATABASE_URL;
const writerOrigins=process.env.WRITER_ORIGINS?.split(',')??[origin];
const credentials=process.env.CREDENTIALS_DIRECTORY??'/run/credentials';
const admin=readFileSync(join(credentials,'admin.token'),'utf8').trim();
const control=Buffer.from(readFileSync(join(credentials,'control.token'),'utf8').trim());
const prefix=process.env.DATABASE_PREFIX;
const status={schema:2,state:'initializing',writes_acknowledged:0,reads_verified:0,http_attempt_statuses:{},failures:[],history:[],expected:[0,0,0,0],verified:[0,0,0,0]};
const names=Array.from({length:4},(_,i)=>`${prefix}-writer-${i}`);
const tokens=[];
let running=false, workers=[], commandBusy=false, issued=0;
const maxIntents=100000;
const tenantLimits={bytes:1024*1024*1024,attempts:100000};
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const hash=value=>createHash('sha256').update(value).digest('hex');
const rpcTiming=()=>({request_seconds:0,backoff_seconds:0,longest_request:{seconds:0,status:null},statuses:{},connections:{}});
async function rpc(path,body,token=admin,method=body===undefined?'GET':'POST',endpoint=origin,timing) {
  const encoded=body===undefined?undefined:JSON.stringify(body), start=performance.now();
  let attempts=0;
  for(;;) {
    const attempted=performance.now();
    const attempt={connection:'unobserved'};
    let code='transport_or_decode_error', result, refusal;
    try {
      const database=path.match(/^\/v1\/databases\/([^/]+)/)?.[1]??'administration';
      result=await activeAttempt.run(attempt,()=>requestJson(endpoint+path,database,{method,headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:encoded,signal:AbortSignal.timeout(25000)}));
      code='200';
    } catch(error) {
      if(error instanceof DbError)code=String(error.status);
      if(code==='413') {
        // Only fixed public refusal categories enter the report.
        refusal=new Map([
          ['database write admission budget exhausted','tenant_funding'],
          ['database quota exceeded','logical_quota'],
          ['read work budget exceeded; narrow the scan','scan_work'],
        ]).get(error.message)??'resource_limit';
      } else if(code==='409') {
        refusal=['conflict','receipt_expired'].includes(error.code)?error.code:'unclassified_conflict';
      }
    }
    status.http_attempt_statuses[code]=(status.http_attempt_statuses[code]??0)+1;
    if(timing) {
      const seconds=(performance.now()-attempted)/1000;
      timing.request_seconds+=seconds;
      if(seconds>timing.longest_request.seconds)timing.longest_request={seconds,status:code};
      timing.statuses[code]=(timing.statuses[code]??0)+1;
      // Keep at most eight socket identities per RPC, plus an overflow bucket.
      const id=attempt.connection;
      const bucket=id in timing.connections || Object.keys(timing.connections).length<8 ? id : 'overflow';
      const counts=timing.connections[bucket]??={};counts[code]=(counts[code]??0)+1;
    }
    if(result!==undefined) return result;
    if(!['transport_or_decode_error','409','429','500','502','503','504'].includes(code)) throw new Error(`Unexpected HTTP status ${code}${refusal?' ('+refusal+')':''}`);
    if(performance.now()-start>180000) {
      const operation=path.endsWith('/commit')?'commit':path.endsWith('/read')?'read':'control';
      throw new Error(`Identical-request recovery deadline exceeded (${operation}, last HTTP status ${code}${refusal?', '+refusal:''})`);
    }
    const waiting=performance.now();
    await sleep(Math.min(++attempts*100,1000));
    if(timing) timing.backoff_seconds+=(performance.now()-waiting)/1000;
  }
}
async function writer(index) {
  const endpoint=writerOrigins[index%writerOrigins.length];
  try {
    while(running) {
      if(issued>=maxIntents)throw new Error('Qualification write-intent bound reached');
      issued++;
      const revision=status.expected[index], request_id=randomUUID();
      const value=Buffer.from(`${index}/${revision}`), key=String(revision).padStart(8,'0');
      const start=performance.now()/1000;
      const commit_rpc=rpcTiming(),read_rpc=rpcTiming();
      const result=await rpc(`/v1/databases/${names[index]}/commit`,{request_id,expected_revision:revision,writes:[{key,value:value.toString('base64')}]},tokens[index],'POST',endpoint,commit_rpc);
      if(result.revision!==revision+1) throw new Error('Acknowledged revision mismatch');
      status.expected[index]=revision+1;status.writes_acknowledged++;
      const record={database_index:index,request_id,revision:revision+1,start,end:performance.now()/1000,readback_end:null,value_sha256:hash(value),commit_rpc,read_rpc};
      status.history.push(record);
      const read=await rpc(`/v1/databases/${names[index]}/read`,{expected_revision:revision+1,keys:[key]},tokens[index],'POST',endpoint,read_rpc);
      if(read.revision!==revision+1||JSON.stringify(read.values)!==JSON.stringify([value.toString('base64')])) throw new Error('Read after acknowledgement mismatch');
      record.readback_end=performance.now()/1000;
      status.reads_verified++;status.verified[index]=revision+1;
      await sleep(250);
    }
  } catch(error) { status.failures.push({worker:index,reason:error.message});running=false; }
}
async function pause() {
  running=false;await Promise.all(workers);workers=[];status.state='paused';
  if(status.failures.length) throw new Error('Workload failed; inspect report');
}
async function verify() {
  if(running||workers.length) throw new Error('Pause the workload before full verification');
  let verified=0;
  for(let index=0;index<names.length;index++) {
    let after;
    const revision=status.expected[index];let count=0;
    for(;;) {
      const result=await rpc(`/v1/databases/${names[index]}/read`,{expected_revision:revision,scans:[{limit:500,...(after===undefined?{}:{after})}]},tokens[index]);
      if(result.revision!==revision) throw new Error('Cold revision mismatch');
      const scan=result.scans[0];
      for(const pair of scan.items) {
        if(pair.key!==String(count).padStart(8,'0')||pair.value!==Buffer.from(`${index}/${count}`).toString('base64')) throw new Error('Cold value mismatch');
        count++;
      }
      if(scan.next===null)break;after=scan.next;
    }
    if(count!==revision)throw new Error('Cold key count mismatch');verified+=count;
  }
  if(verified!==status.writes_acknowledged)throw new Error('Cold acknowledgement total mismatch');
  status.verified_keys=verified;return {verified_keys:verified};
}
function authorized(request) {
  const supplied=Buffer.from((request.headers.authorization??'').replace(/^Bearer /,''));
  return supplied.length===control.length&&timingSafeEqual(supplied,control);
}
const server=createServer(async(request,response)=>{
  response.setHeader('content-type','application/json');
  if(!authorized(request)){response.writeHead(401).end('{}');return;}
  if(request.method==='GET'&&request.url==='/status'){
    response.end(JSON.stringify({...status,history:undefined,history_length:status.history.length,write_intents:issued,monotonic_seconds:performance.now()/1000}));return;
  }
  if(request.method==='GET'&&request.url==='/status?history=1'){response.end(JSON.stringify(status));return;}
  if(commandBusy){response.writeHead(409).end('{}');return;}
  commandBusy=true;
  try {
    if(request.method!=='POST')throw new Error('Unsupported command');
    let result;
    if(request.url==='/resume') {
      if(status.state!=='paused'||status.failures.length)throw new Error('Workload is not ready');
      running=true;status.state='running';workers=names.map((_,i)=>writer(i));result={running:true};
    } else if(request.url==='/pause'){await pause();result={paused:true};}
    else if(request.url==='/verify')result=await verify();
    else if(request.url==='/budget')result=await rpc('/v1/storage/upload-budget');
    else if(request.url==='/tenant-budgets')result=await Promise.all(names.map(name=>rpc(`/v1/databases/${name}/upload-budget`)));
    else throw new Error('Unsupported command');
    response.end(JSON.stringify(result));
  } catch(error){response.writeHead(500).end(JSON.stringify({error:error.message}));}
  finally{commandBusy=false;}
}).listen(Number(process.env.CONTROL_PORT??9001),'0.0.0.0',()=>{
  process.send?.({control_port:server.address().port});
});
try {
  for(const name of names) {
    await rpc(`/v1/databases/${name}/upload-budget`,{request_id:'fund-campaign',expected_configuration_revision:0,limits:tenantLimits});
    await rpc(`/v1/databases/${name}`,{request_id:randomUUID(),limits:{max_bytes:16*1024*1024,max_keys:100000}},admin,'PUT');
    tokens.push((await rpc('/v1/tokens',{database:name,write:true,ttl_seconds:14400})).token);
  }
  status.state='paused';
} catch(error){status.state='failed';status.failures.push({worker:'initialization',reason:error.message});}
