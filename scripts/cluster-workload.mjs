// Synthetic sequential append workload for the isolated multi-node qualifier.
// No tokens, keys or stored values are written to logs or the report.
import {readFileSync} from 'node:fs';
import {createServer} from 'node:http';
import {randomUUID, createHash, timingSafeEqual} from 'node:crypto';
import {performance} from 'node:perf_hooks';
const origin=process.env.DATABASE_URL;
const admin=readFileSync('/run/credentials/admin.token','utf8').trim();
const control=Buffer.from(readFileSync('/run/credentials/control.token','utf8').trim());
const prefix=process.env.DATABASE_PREFIX;
const status={schema:1,state:'initializing',writes_acknowledged:0,reads_verified:0,http_attempt_statuses:{},failures:[],history:[],expected:[0,0,0,0],verified:[0,0,0,0]};
const names=Array.from({length:4},(_,i)=>`${prefix}-writer-${i}`);
const tokens=[];
let running=false, workers=[], commandBusy=false, issued=0;
const maxIntents=100000;
const tenantLimits={bytes:1024*1024*1024,attempts:100000};
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const hash=value=>createHash('sha256').update(value).digest('hex');
async function rpc(path,body,token=admin,method=body===undefined?'GET':'POST') {
  const encoded=body===undefined?undefined:JSON.stringify(body), start=performance.now();
  let attempts=0;
  for(;;) {
    let code='transport_or_decode_error', result, refusal;
    try {
      const response=await fetch(origin+path,{method,headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:encoded,redirect:'error',signal:AbortSignal.timeout(25000)});
      code=String(response.status);
      if(response.ok) result=await response.json();
      else if(response.status===413) {
        const error=await response.json();
        // Only fixed public refusal categories enter the report.
        refusal=new Map([
          ['database write admission budget exhausted','tenant_funding'],
          ['database quota exceeded','logical_quota'],
          ['read work budget exceeded; narrow the scan','scan_work'],
        ]).get(error.message)??'resource_limit';
      } else if(response.status===409) {
        const error=await response.json();
        refusal=['conflict','receipt_expired'].includes(error.code)?error.code:'unclassified_conflict';
      } else await response.body?.cancel();
    } catch { code='transport_or_decode_error'; }
    status.http_attempt_statuses[code]=(status.http_attempt_statuses[code]??0)+1;
    if(result!==undefined) return result;
    if(!['transport_or_decode_error','409','429','500','502','503','504'].includes(code)) throw new Error(`Unexpected HTTP status ${code}${refusal?' ('+refusal+')':''}`);
    if(performance.now()-start>180000) {
      const operation=path.endsWith('/commit')?'commit':path.endsWith('/read')?'read':'control';
      throw new Error(`Identical-request recovery deadline exceeded (${operation}, last HTTP status ${code}${refusal?', '+refusal:''})`);
    }
    await sleep(Math.min(++attempts*100,1000));
  }
}
async function writer(index) {
  try {
    while(running) {
      if(issued>=maxIntents)throw new Error('Qualification write-intent bound reached');
      issued++;
      const revision=status.expected[index], request_id=randomUUID();
      const value=Buffer.from(`${index}/${revision}`), key=String(revision).padStart(8,'0');
      const start=performance.now()/1000;
      const result=await rpc(`/v1/databases/${names[index]}/commit`,{request_id,expected_revision:revision,writes:[{key,value:value.toString('base64')}]},tokens[index]);
      if(result.revision!==revision+1) throw new Error('Acknowledged revision mismatch');
      status.expected[index]=revision+1;status.writes_acknowledged++;
      status.history.push({database_index:index,request_id,revision:revision+1,start,end:performance.now()/1000,value_sha256:hash(value)});
      const read=await rpc(`/v1/databases/${names[index]}/read`,{expected_revision:revision+1,keys:[key]},tokens[index]);
      if(read.revision!==revision+1||JSON.stringify(read.values)!==JSON.stringify([value.toString('base64')])) throw new Error('Read after acknowledgement mismatch');
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
createServer(async(request,response)=>{
  response.setHeader('content-type','application/json');
  if(!authorized(request)){response.writeHead(401).end('{}');return;}
  if(request.method==='GET'&&request.url==='/status'){
    response.end(JSON.stringify({...status,history:undefined,history_length:status.history.length,write_intents:issued}));return;
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
}).listen(9001,'0.0.0.0');
try {
  for(const name of names) {
    await rpc(`/v1/databases/${name}/upload-budget`,{request_id:'fund-campaign',expected_configuration_revision:0,limits:tenantLimits});
    await rpc(`/v1/databases/${name}`,{request_id:randomUUID(),limits:{max_bytes:16*1024*1024,max_keys:100000}},admin,'PUT');
    tokens.push((await rpc('/v1/tokens',{database:name,write:true,ttl_seconds:14400})).token);
  }
  status.state='paused';
} catch(error){status.state='failed';status.failures.push({worker:'initialization',reason:error.message});}
