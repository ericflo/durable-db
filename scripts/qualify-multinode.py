#!/usr/bin/env python3
"""Run isolated replica-loss and provider-egress faults in a new test namespace.

Never drains/reboots hosts or edits cluster-wide objects. Requires an existing
cluster, immutable database image, dedicated disposable provider namespace,
and private storage/signing/administrator files. The caller owns provider cleanup.
"""
import argparse
import base64
import datetime
import hashlib
import json
from pathlib import Path
import re
import secrets
import subprocess
import time
import urllib.request
import uuid
import yaml

NODE_IMAGE = 'docker.io/library/node:24-bookworm-slim@sha256:c0a93797ee78814ae6d32fcca2561fdbbe423149c3f5389aad32d86e9fd85bd4'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True)
    parser.add_argument('--image', required=True)
    parser.add_argument('--storage-env-file', type=Path, required=True)
    parser.add_argument('--signing-key-file', type=Path, required=True)
    parser.add_argument('--admin-token-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--control-port', type=int, default=17840)
    parser.add_argument('--rollout-surge', type=int, choices=(1, 2, 3), default=1,
                        help='Concurrent replacement headroom within the fixed test quota')
    parser.add_argument('--steady-seconds', type=int, default=0,
                        help='Optional bounded six-replica pilot, at most one hour, before disruption')
    parser.add_argument('--observe-memory', action='store_true',
                        help='Read only this namespace from kubelet statistics through the API proxy')
    parser.add_argument('--disposable', action='store_true', required=True)
    args = parser.parse_args()
    if not 0 <= args.steady_seconds <= 3600:
        parser.error('The bounded pilot duration must be between zero and 3600 seconds')
    if args.observe_memory and not args.steady_seconds:
        parser.error('Memory observation requires a nonzero steady pilot duration')
    if not re.search(r'@sha256:[0-9a-f]{64}$',args.image):
        parser.error('Pin the database image by its immutable SHA-256 digest')
    run = uuid.uuid4().hex[:12]
    namespace = 'durable-db-qualification-' + run
    control = secrets.token_hex(32)
    storage = json.loads(args.storage_env_file.read_text())
    if ('DURABLE_DB_BUCKET' in storage) == ('DURABLE_DB_B2_BUCKET' in storage):
        parser.error('Provide exactly one provider for a dedicated disposable namespace')
    report = dict(schema=1, complete=False, started_at=utc(), context=args.context,
                  namespace=namespace, image=args.image, source_commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                  harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  workload_sha256=hashlib.sha256(Path('scripts/cluster-workload.mjs').read_bytes()).hexdigest(),
                  configuration=dict(cpu_request='25m', cpu_limit='250m', memory_request='96Mi', memory_limit='384Mi',
                                     client_origin='http://durable-db:7654',
                                     memory_cache_mib=8, resident_memory_mib=128, partitions=16, discovery='headless DNS', disk_cache=False, maintenance_seconds=60,
                                     rollout_surge=args.rollout_surge, rollout_unavailable=0, rollout_deadline_seconds=240,
                                     upload_budget=dict(bytes=1073741824, attempts=100000),
                                     tenant_upload_budget=dict(bytes=1073741824, attempts=100000),
                                     steady_seconds=args.steady_seconds, memory_observation=args.observe_memory,
                                     maximum_write_intents=100000),
                  phases=[], budget_observations=[], resource_samples=[], failures=[], limitations=[
                      'Manual replica changes; no CPU-driven HPA claim.',
                      'Application-pod loss and namespace-scoped egress restriction; no host shutdown, node drain, rack failure or control-plane failure.',
                      'Short sequential append fixture; no production SLO, arbitrary contention linearizability, sustained-load or AWS claim.'])
    args.output.parent.mkdir(parents=True,exist_ok=True)
    forward = None
    created = False

    def kube(*command, body=None, timeout=60):
        return subprocess.check_output(['kubectl','--context',args.context,'-n',namespace,*command],
                                       input=None if body is None else json.dumps(body).encode(),stderr=subprocess.PIPE,timeout=timeout)

    def save():
        args.output.write_text(json.dumps(report,indent=2)+'\n')

    def call(endpoint, body=None, timeout=240):
        encoded = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f'http://127.0.0.1:{args.control_port}'+endpoint,
                                         data=encoded,headers={'Authorization':'Bearer '+control,'Content-Type':'application/json'})
        return json.load(urllib.request.urlopen(request,timeout=timeout))

    def snapshot():
        state = call('/status', timeout=10)
        if state['failures']:
            report['failures'] = state['failures']
            save()
            raise RuntimeError('Synthetic workload failed')
        return state

    def phase(name, **details):
        acquisitions()
        report['phases'].append(dict(name=name,time_utc=utc(),**details))
        save()
        print(json.dumps(report['phases'][-1]),flush=True)

    def pods():
        return json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']

    def acquisitions():
        # Preserve bounded progress from earlier pods before rolling deletion.
        # Fixed fields only; provider messages may contain secrets or data.
        samples = report.setdefault('acquisition_events', {})
        for pod in pods():
            name = pod['metadata']['name']
            try:
                log = kube('logs', name, '--tail=256').decode(errors='replace')
                log = re.sub(r'\x1b\[[0-9;]*m', '', log)
                samples[name] = [dict(time_utc=stamp, outcome=outcome,
                    partition=int(partition), stage=stage, elapsed_ms=int(elapsed))
                    for stamp, outcome, partition, stage, elapsed in re.findall(
                        r'(?m)^([0-9TZ:.+\-]+).*?native acquisition (progress|failed).*?partition=(\d+)\s+stage="?(resident|budget|observe|claim|activate|select_journals|recover|serve|ready)"?\b.*?elapsed_ms=(\d+)', log)]
            except subprocess.SubprocessError:
                pass  # A disappearing pod must not prevent the failure report.

    def wait_replicas(count):
        deadline = time.monotonic()+240
        while time.monotonic()<deadline:
            live = pods()
            active = [p for p in live if not p['metadata'].get('deletionTimestamp')]
            ready = [p for p in active if p['status'].get('containerStatuses') and all(c.get('ready') for c in p['status']['containerStatuses'])]
            if len(active)==count and len(ready)==count and (count or not live):
                if count>=3 and len({p['spec']['nodeName'] for p in ready})<3:
                    raise RuntimeError('Expected API placement across three distinct nodes')
                return ready
            time.sleep(2)
        raise RuntimeError('Replica readiness deadline exceeded')

    def placement(ready):
        return [dict(pod=p['metadata']['name'],node=p['spec']['nodeName'],
                     image_id=p['status']['containerStatuses'][0]['imageID']) for p in ready]

    def budget(stage):
        status = call('/budget',{})
        if status['limits']!=report['configuration']['upload_budget'] or status['configuration_revision']!=0:
            raise RuntimeError('Upload ceilings changed')
        earlier = report['budget_observations']
        if any(status['allocated'][k]>status['limits'][k] or (earlier and status['allocated'][k]<earlier[-1]['status']['allocated'][k]) for k in ('bytes','attempts')):
            raise RuntimeError('Upload allocation decreased or exceeded its ceiling')
        tenants=call('/tenant-budgets',{})
        if len(tenants)!=4:raise RuntimeError('Expected four tenant budgets')
        for index,tenant in enumerate(tenants):
            if tenant['limits']!=report['configuration']['tenant_upload_budget'] or tenant['configuration_revision']!=1:
                raise RuntimeError('Tenant upload ceilings changed')
            if any(tenant['allocated'][k]>tenant['limits'][k] or (earlier and tenant['allocated'][k]<earlier[-1]['tenants'][index]['allocated'][k]) for k in ('bytes','attempts')):
                raise RuntimeError('Tenant allocation decreased or exceeded its ceiling')
        earlier.append(dict(stage=stage,status=status,tenants=tenants));save()

    def progress(additional=8):
        start = snapshot()['writes_acknowledged'];deadline=time.monotonic()+240
        while time.monotonic()<deadline:
            now=snapshot()['writes_acknowledged']
            if now>=start+additional:return now
            time.sleep(3)
        raise RuntimeError('Acknowledged-write progress deadline exceeded')

    def sample_resources():
        sample = dict(time_utc=utc(), pods=[])
        ready = pods()
        for node in sorted({p['spec']['nodeName'] for p in ready if 'nodeName' in p['spec']}):
            # Kubelet summaries contain aggregate resource counters, not pod
            # credentials or application data. Keep only this task's namespace.
            summary = json.loads(kube('get','--raw',f'/api/v1/nodes/{node}/proxy/stats/summary'))
            for pod in summary.get('pods',[]):
                ref = pod['podRef']
                if ref['namespace'] != namespace or not any(ref['uid'] == p['metadata']['uid'] for p in ready):
                    continue
                for container in pod.get('containers',[]):
                    if container['name'] != 'database':continue
                    memory, cpu = container.get('memory',{}), container.get('cpu',{})
                    sample['pods'].append(dict(pod=ref['name'],uid=ref['uid'],node=node,
                        memory_observed_at=memory.get('time'),cpu_observed_at=cpu.get('time'),
                        rss_bytes=memory.get('rssBytes'),working_set_bytes=memory.get('workingSetBytes'),
                        usage_bytes=memory.get('usageBytes'),cpu_usage_nanoseconds=cpu.get('usageCoreNanoSeconds'),
                        restart_count=next((c['restartCount'] for p in ready if p['metadata']['uid']==ref['uid']
                            for c in p['status'].get('containerStatuses',[]) if c['name']=='database'),None)))
        expected={p['metadata']['uid'] for p in ready if not p['metadata'].get('deletionTimestamp')}
        if {p['uid'] for p in sample['pods']} != expected or not expected or any(p['rss_bytes'] is None or p['working_set_bytes'] is None for p in sample['pods']):
            report['incomplete_resource_sample']=sample;save()
            raise RuntimeError('Kubelet did not supply required task memory counters')
        report['resource_samples'].append(sample);save()
        return sample

    policy = dict(apiVersion='networking.k8s.io/v1',kind='NetworkPolicy',metadata=dict(name='storage-client'),
                  spec=dict(podSelector=dict(matchLabels={'durable-db-role':'storage-client'}),policyTypes=['Ingress','Egress'],
                            ingress=[dict(**{'from':[dict(podSelector=dict(matchLabels={'app':'durable-db-runner'})),dict(podSelector=dict(matchLabels={'app':'durable-db'}))]},ports=[dict(protocol='TCP',port=7654)])],
                            egress=[dict(ports=[dict(protocol='UDP',port=53),dict(protocol='TCP',port=53)]),
                                    dict(to=[dict(podSelector=dict(matchLabels={'app':'durable-db'}))],ports=[dict(protocol='TCP',port=7654)]),
                                    dict(to=[dict(ipBlock=dict(cidr='0.0.0.0/0'))],ports=[dict(protocol='TCP',port=443)])]))

    def provider_probe(expect_allowed):
        name='provider-probe-'+uuid.uuid4().hex[:6]
        script="const https=require('https');const r=https.get('https://api.backblazeb2.com/',x=>{x.resume();console.log('CONNECTED');process.exit(0)});r.setTimeout(8000,()=>r.destroy());r.on('error',()=>{console.log('BLOCKED');process.exit(0)});setTimeout(()=>{console.log('BLOCKED');process.exit(0)},10000);"
        pod=dict(apiVersion='v1',kind='Pod',metadata=dict(name=name,labels={'durable-db-role':'storage-client'}),
                 spec=dict(restartPolicy='Never',automountServiceAccountToken=False,
                           securityContext=dict(runAsNonRoot=True,runAsUser=1000,runAsGroup=1000,seccompProfile=dict(type='RuntimeDefault')),
                           containers=[dict(name='probe',image=NODE_IMAGE,command=['node','-e',script],
                                            resources=dict(requests=dict(cpu='5m',memory='32Mi'),limits=dict(cpu='50m',memory='64Mi')),
                                            securityContext=dict(allowPrivilegeEscalation=False,readOnlyRootFilesystem=True,capabilities=dict(drop=['ALL'])))]))
        kube('apply','-f','-',body=pod)
        try:
            deadline=time.monotonic()+90
            while time.monotonic()<deadline:
                state=json.loads(kube('get','pod',name,'-o','json'))['status']['phase']
                if state=='Succeeded':break
                if state=='Failed':raise RuntimeError('Provider probe container failed')
                time.sleep(2)
            else:raise RuntimeError('Provider probe deadline exceeded')
            result=kube('logs',name).decode().strip()
            if result!=('CONNECTED' if expect_allowed else 'BLOCKED'):
                raise RuntimeError('Provider reachability does not match NetworkPolicy fault')
            return result
        finally:kube('delete','pod',name,'--wait=false')

    save()
    try:
        if kube('get','namespace',namespace,'--ignore-not-found','-o','name').strip():
            raise RuntimeError('Test namespace already exists')
        ready_nodes=[n for n in json.loads(kube('get','nodes','-o','json'))['items'] if not n['spec'].get('unschedulable') and any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions'])]
        if len(ready_nodes)<3:raise RuntimeError('Three ready schedulable nodes are required')
        report['nodes']=[n['metadata']['name'] for n in ready_nodes]
        report['kubernetes_version']=json.loads(kube('version','-o','json'))['serverVersion']['gitVersion']
        kube('create','-f','-',body=dict(apiVersion='v1',kind='Namespace',metadata=dict(name=namespace,labels={'durable-db-qualification':run})))
        created=True
        resources=[dict(apiVersion='v1',kind='ResourceQuota',metadata=dict(name='qualification'),spec=dict(hard={'requests.cpu':'1','limits.cpu':'3','requests.memory':'3Gi','limits.memory':'4Gi','pods':'10'})),
                   dict(apiVersion='v1',kind='Secret',metadata=dict(name='durable-db-storage'),stringData=storage),
                   dict(apiVersion='v1',kind='Secret',metadata=dict(name='durable-db-signing'),stringData={'admin.key':args.signing_key_file.read_text()}),
                   dict(apiVersion='v1',kind='Secret',metadata=dict(name='workload-credentials'),stringData={'admin.token':args.admin_token_file.read_text(),'control.token':control}),
                   dict(apiVersion='v1',kind='ConfigMap',metadata=dict(name='workload'),data={'runner.mjs':Path('scripts/cluster-workload.mjs').read_text()}),policy]
        base=subprocess.check_output(['kubectl','kustomize','deploy/kubernetes'],text=True)
        for resource in yaml.safe_load_all(base):
            if resource['kind'] not in ('Deployment','Service','PodDisruptionBudget'):continue
            if resource['kind']=='Deployment':
                resource['spec']['strategy']['rollingUpdate']['maxSurge']=args.rollout_surge
                template=resource['spec']['template'];template['metadata']['labels']['durable-db-role']='storage-client'
                template['spec']['topologySpreadConstraints'][0]['whenUnsatisfiable']='DoNotSchedule'
                container=template['spec']['containers'][0];container['image']=args.image
                container['args']=['serve','--listen=0.0.0.0:7654','--admin-key-file=/run/keys/admin.key','--memory-cache-mib=8','--concurrency=4','--maintenance-seconds=60','--upload-budget-bytes=1073741824','--upload-budget-attempts=100000']
                container['resources']=dict(requests=dict(cpu='25m',memory='96Mi'),limits=dict(cpu='250m',memory='384Mi'))
                container['volumeMounts']=[m for m in container['volumeMounts'] if m['name']=='keys']
                template['spec']['volumes']=[v for v in template['spec']['volumes'] if v['name']=='keys']
            resources.append(resource)
        for resource in resources:resource['metadata']['namespace']=namespace
        kube('apply','-f','-',body=dict(apiVersion='v1',kind='List',items=resources))
        initial=wait_replicas(1);phase('initial-one',placement=[p['spec']['nodeName'] for p in initial])
        phase('provider-reachable',probe=provider_probe(True))
        runner=dict(apiVersion='v1',kind='Pod',metadata=dict(name='workload',labels={'app':'durable-db-runner'}),
                    spec=dict(restartPolicy='Never',automountServiceAccountToken=False,
                              securityContext=dict(runAsNonRoot=True,runAsUser=1000,runAsGroup=1000,fsGroup=1000,seccompProfile=dict(type='RuntimeDefault')),
                              containers=[dict(name='workload',image=NODE_IMAGE,command=['node','/work/runner.mjs'],
                                               env=[dict(name='DATABASE_URL',value='http://durable-db:7654'),dict(name='DATABASE_PREFIX',value='physical-'+run)],
                                               resources=dict(requests=dict(cpu='25m',memory='64Mi'),limits=dict(cpu='250m',memory='256Mi')),
                                               securityContext=dict(allowPrivilegeEscalation=False,readOnlyRootFilesystem=True,capabilities=dict(drop=['ALL'])),
                                               volumeMounts=[dict(name='work',mountPath='/work',readOnly=True),dict(name='credentials',mountPath='/run/credentials',readOnly=True)])],
                              volumes=[dict(name='work',configMap=dict(name='workload')),dict(name='credentials',secret=dict(secretName='workload-credentials',defaultMode=0o440))]))
        kube('apply','-f','-',body=runner)
        kube('wait','--for=condition=Ready','pod/workload','--timeout=120s',timeout=150)
        with args.output.with_suffix('.forward.log').open('w') as log:
            forward=subprocess.Popen(['kubectl','--context',args.context,'-n',namespace,'port-forward','pod/workload',f'{args.control_port}:9001'],stdout=log,stderr=log)
        deadline=time.monotonic()+240
        while time.monotonic()<deadline:
            if forward.poll() is not None:raise RuntimeError('Control port forward stopped')
            try:
                state=snapshot()
                if state['state']=='paused':break
                if state['state']=='failed':raise RuntimeError('Workload initialization failed')
            except (OSError,ValueError):pass
            time.sleep(2)
        else:raise RuntimeError('Workload readiness deadline exceeded')
        budget('initial-one');call('/resume',{})
        phase('one-progress',writes_acknowledged=progress())
        for count in (3,6):
            kube('scale','deployment','durable-db',f'--replicas={count}')
            located=placement(wait_replicas(count));budget(f'scale-{count}')
            phase(f'scale-{count}',placement=located,writes_acknowledged=progress())
        if args.steady_seconds:
            start=time.monotonic();last_progress=start;previous=snapshot()['writes_acknowledged']
            steady_uids={p['metadata']['uid'] for p in pods() if not p['metadata'].get('deletionTimestamp')}
            phase('steady-start',seconds=args.steady_seconds,writes_acknowledged=previous)
            while time.monotonic()-start < args.steady_seconds:
                state=snapshot();now=time.monotonic()
                if state['writes_acknowledged']>previous:
                    previous=state['writes_acknowledged'];last_progress=now
                if now-last_progress>180:raise RuntimeError('Steady workload stopped making progress')
                if args.observe_memory:
                    sample=sample_resources()
                    if {p['uid'] for p in sample['pods']}!=steady_uids or any(p['restart_count']!=0 for p in sample['pods']):
                        raise RuntimeError('An unplanned API restart or replacement interrupted the memory pilot')
                if int(now-start)//60 > int(report.get('steady_observed_seconds',0))//60:
                    budget('steady-'+str(int(now-start)))
                report['steady_observed_seconds']=now-start;save()
                time.sleep(min(15,max(0,args.steady_seconds-(time.monotonic()-start))))
            phase('steady-complete',elapsed_seconds=time.monotonic()-start,writes_acknowledged=snapshot()['writes_acknowledged'])
        victim=pods()[0]
        kube('delete','pod',victim['metadata']['name'],'--grace-period=0','--force','--wait=false')
        phase('forced-api-pod-loss',victim_node=victim['spec']['nodeName'],placement=placement(wait_replicas(6)),writes_acknowledged=progress())
        kube('rollout','restart','deployment/durable-db')
        kube('rollout','status','deployment/durable-db','--timeout=240s',timeout=270)
        phase('rolling-replacement',placement=placement(wait_replicas(6)),writes_acknowledged=progress())
        restricted=json.loads(json.dumps(policy));restricted['spec']['egress']=restricted['spec']['egress'][:-1]
        kube('apply','-f','-',body=restricted)
        phase('provider-partition-start',probe=provider_probe(False))
        time.sleep(40)
        kube('apply','-f','-',body=policy)
        phase('provider-partition-healed',probe=provider_probe(True),writes_acknowledged=progress())
        kube('scale','deployment','durable-db','--replicas=1')
        phase('scale-one',placement=placement(wait_replicas(1)),writes_acknowledged=progress())
        budget('scale-one')
        # Writers remain active: pending mutations resolve after the cold restart.
        kube('scale','deployment','durable-db','--replicas=0');wait_replicas(0)
        phase('zero-api-pods',writes_acknowledged=snapshot()['writes_acknowledged'])
        time.sleep(5)
        kube('scale','deployment','durable-db','--replicas=3')
        phase('cold-three',placement=placement(wait_replicas(3)),writes_acknowledged=progress())
        call('/pause',{});call('/verify',{});budget('cold-three')
        state=call('/status?history=1',timeout=60);history=state.pop('history')
        raw=''.join(json.dumps(record,separators=(',',':'))+'\n' for record in history).encode()
        args.output.with_suffix('.history.jsonl').write_bytes(raw)
        report.update(state);report['history_sha256']=hashlib.sha256(raw).hexdigest();report['complete']=True
    except BaseException as error:
        report['controller_failure']=dict(type=type(error).__name__,reason=str(error))
        if created:
            try:
                current=pods()
                report['pod_states']=[dict(name=p['metadata']['name'],node=p['spec'].get('nodeName'),phase=p['status']['phase'],conditions=p['status'].get('conditions',[]),containers=[dict(name=c['name'],restart_count=c['restartCount'],state=c['state'],last_state=c.get('lastState')) for c in p['status'].get('containerStatuses',[])]) for p in current]
                acquisitions()
                events=json.loads(kube('get','events','-o','json'))['items']
                report['event_reasons']=[dict(reason=e.get('reason'),message=e.get('message')) for e in events]
            except Exception:pass
        save()
        if forward and forward.poll() is None:
            try:report['partial_workload']=call('/status?history=1',timeout=30)
            except Exception:pass
            try:report['final_tenant_budgets']=call('/tenant-budgets',{},timeout=60)
            except Exception:pass
        raise
    finally:
        if forward and forward.poll() is None:forward.terminate();forward.wait(timeout=15)
        if created:
            # The namespace is fresh and task-owned; no other resources are deleted.
            kube('delete','namespace',namespace,'--wait=true','--timeout=180s',timeout=210)
            report['namespace_removed']=True
        report['finished_at']=utc();save()
    print(json.dumps(dict(complete=report['complete'],writes_acknowledged=report['writes_acknowledged'],verified_keys=report['verified_keys'])),flush=True)


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


if __name__=='__main__':
    main()
