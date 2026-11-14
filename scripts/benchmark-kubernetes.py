#!/usr/bin/env python3
"""Run benchmark-http on three pods in a fresh disposable namespace.

Reuses deploy/kubernetes and the SDK benchmark; samples only these database
containers. Caller supplies a dedicated bucket and owns provider cleanup.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import runpy
import time
import uuid

import yaml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['context', 'image', 'storage-env-file', 'signing-key-file', 'admin-token-file', 'output']:
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--concurrency', type=int, default=8)
    parser.add_argument('--value-bytes', type=int, default=1024)
    parser.add_argument('--databases', type=int, default=32)
    parser.add_argument('--workers', default='1,4,16,32')
    parser.add_argument('--operations', type=int, default=20)
    parser.add_argument('--cold-recovery', action='store_true',
                        help='Freeze all database processes, replace every pod, then verify values, receipts and new writes')
    parser.add_argument('--disposable', action='store_true', required=True)
    args = parser.parse_args()
    if not 1<=args.concurrency<=128 or not 32<=args.value_bytes<=65536:
        parser.error('Concurrency must be 1..128 and values 32..65536 bytes')
    if not 1<=args.databases<=100000 or not 1<=args.operations<=10000:
        parser.error('Invalid database or operation count')
    if not all(w.isdecimal() and 1<=int(w)<=min(args.databases,128) for w in args.workers.split(',')):
        parser.error('Invalid worker counts')
    import re
    if not re.search(r'@sha256:[0-9a-f]{64}$', args.image):
        parser.error('Pin the database image by digest')
    storage = json.loads(Path(args.storage_env_file).read_text())
    if ('DURABLE_DB_BUCKET' in storage) == ('DURABLE_DB_B2_BUCKET' in storage):
        parser.error('Supply exactly one dedicated storage provider')
    output = Path(args.output)
    output.mkdir(exist_ok=False, parents=True)
    namespace = 'durable-db-benchmark-'+uuid.uuid4().hex[:12]
    now = lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    report = dict(complete=False, started_at=now(), namespace=namespace, image=args.image,
                  source_commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                  scripts_sha256={name:hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in
                      [__file__,'scripts/benchmark-http.mjs','clients/javascript/transport.mjs']},
                  configuration=dict(replicas=3, partitions=16, concurrency_per_process=args.concurrency,
                      value_bytes=args.value_bytes,
                      memory_cache_mib=64, resident_memory_mib=128, disk_cache=False,
                      cpu_limit='2', memory_limit='768Mi', maintenance_seconds=60,
                      namespace_upload_budget=dict(bytes=1073741824,attempts=100000),
                      resource_sample_interval_seconds=10), resource_samples=[])
    fixture = runpy.run_path('scripts/qualify-multinode.py')
    node_image = fixture['NODE_IMAGE']
    created = False
    load = None

    def kube(*cmd, body=None, timeout=60):
        return subprocess.check_output(['kubectl','--context',args.context,'-n',namespace,*cmd],
            input=None if body is None else json.dumps(body).encode(), stderr=subprocess.PIPE, timeout=timeout)

    def save():
        (output/'resources.json').write_text(json.dumps(report,indent=2)+'\n')

    def journal_counters():
        # Use the workload's mounted token; only fixed counters leave the pod.
        peers = [dict(pod=p['metadata']['name'], host=p['status']['podIP']) for p in
                 json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']]
        script = r"""const fs=require('fs');const token=fs.readFileSync('/run/credentials/admin.token','utf8').trim();
Promise.all(JSON.parse(process.argv[1]).map(async ({pod,host})=>{
 try {
  const r=await fetch(`http://${host.includes(':')?'['+host+']':host}:7654/metrics`,{headers:{authorization:'Bearer '+token},redirect:'error',signal:AbortSignal.timeout(10000)});
  if(!r.ok) return {pod,status:r.status};
  const body=await r.text();if(body.length>1048576)throw Error();
  const count=name=>{const m=body.match(new RegExp('^durable_db_events_total\\{event="journal_'+name+'_prepared"\\} ([0-9]+)$','m'));return m?Number(m[1]):null;};
  return {pod,status:200,frames:count('frames'),records:count('records')};
 }catch{return {pod,status:'unreachable'};}
})).then(rows=>console.log(JSON.stringify(rows)));"""
        return dict(time_utc=now(),pods=json.loads(kube('exec','benchmark','--','node','-e',script,json.dumps(peers),timeout=20)))

    def sample():
        pods = json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
        observed = []
        for node in sorted({p['spec']['nodeName'] for p in pods}):
            summary = json.loads(kube('get','--raw',f'/api/v1/nodes/{node}/proxy/stats/summary'))
            for pod in summary.get('pods',[]):
                ref = pod['podRef']
                if ref['namespace'] != namespace: continue
                current = next((p for p in pods if p['metadata']['uid']==ref['uid']),None)
                if not current: continue
                for container in pod.get('containers',[]):
                    if container['name']!='database': continue
                    memory, cpu = container.get('memory',{}),container.get('cpu',{})
                    observed.append(dict(pod=ref['name'],uid=ref['uid'],node=node,
                        memory_observed_at=memory.get('time'),cpu_observed_at=cpu.get('time'),
                        rss_bytes=memory.get('rssBytes'),working_set_bytes=memory.get('workingSetBytes'),
                        usage_bytes=memory.get('usageBytes'),cpu_usage_nanoseconds=cpu.get('usageCoreNanoSeconds'),
                        restart_count=next(c['restartCount'] for c in current['status']['containerStatuses'] if c['name']=='database')))
        epoch = 'cold' if report.get('cold_recovery',{}).get('pods_ready_at') else 'warm'
        report['resource_samples'].append(dict(time_utc=now(),epoch=epoch,pods=observed)); save()
        if len(observed)!=3 or any(p['rss_bytes'] is None or p['cpu_usage_nanoseconds'] is None or p['restart_count'] for p in observed):
            raise RuntimeError('Incomplete counters or an unplanned database restart')
        first = next(s for s in report['resource_samples'] if s['epoch']==epoch)
        if {p['uid'] for p in observed}!={p['uid'] for p in first['pods']}:
            raise RuntimeError('Database pod replaced during baseline')

    def cold_restart(barrier):
        before=json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
        if len(before)!=3 or {p['metadata']['uid'] for p in before}!={p['uid'] for p in report['resource_samples'][0]['pods']}:
            raise RuntimeError('Expected every original warm database pod')
        if any(c['restartCount'] for p in before for c in p['status']['containerStatuses']):
            raise RuntimeError('Warm container restarted before the cold fault')
        cold=report['cold_recovery']=dict(started_at=now(),old_pods=[dict(name=p['metadata']['name'],uid=p['metadata']['uid']) for p in before])
        report['journal_counters_end']=journal_counters();save()
        def stop(pod):
            name=pod['metadata']['name']
            state=json.loads(kube('exec',name,'-c','fault','--','node','-e',fixture['PROCESS_CONTROL'],'STOP'))
            if state['state']!='T':raise RuntimeError('Database did not stop')
            return dict(pod=name,**state)
        # Freeze before deletion so SIGTERM cannot flush away the long suffix.
        # Same-UID control targets exactly one non-init database process per pod.
        with ThreadPoolExecutor(max_workers=3) as pool:cold['stopped_processes']=list(pool.map(stop,before))
        for state in cold['stopped_processes']:
            current=json.loads(kube('exec',state['pod'],'-c','fault','--','node','-e',fixture['PROCESS_CONTROL'],
                                    'STATE',str(state['pid']),state['started']))
            if current['state']!='T':raise RuntimeError('Frozen database resumed')
            (output/(state['pod']+'-before-cold.log')).write_bytes(kube('logs',state['pod'],'-c','database','--tail=10000'))
        frozen=json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
        if {p['metadata']['uid'] for p in frozen}!={p['metadata']['uid'] for p in before} or any(c['restartCount'] for p in frozen for c in p['status']['containerStatuses']):
            raise RuntimeError('Frozen pod was replaced or restarted')
        cold['all_stopped_at']=now();save()
        kube('scale','statefulset','durable-db','--replicas=0')
        deadline=time.monotonic()+60
        while json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']:
            if time.monotonic()>deadline:raise RuntimeError('Frozen database pods did not terminate')
            time.sleep(1)
        cold['zero_pods_at']=now();save()
        kube('scale','statefulset','durable-db','--replicas=3')
        kube('rollout','status','statefulset/durable-db','--timeout=240s',timeout=270)
        after=json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
        if len(after)!=3 or {p['metadata']['uid'] for p in before}&{p['metadata']['uid'] for p in after}:
            raise RuntimeError('Cold restart retained an old pod')
        if len({p['spec']['nodeName'] for p in after})!=3:raise RuntimeError('Cold pods must use three hosts')
        cold['new_pods']=[dict(name=p['metadata']['name'],uid=p['metadata']['uid']) for p in after]
        cold['pods_ready_at']=now();cold['journal_counters_start']=journal_counters();save()
        script="const fs=require('fs');fs.writeFileSync('/output/recovery.resume.tmp',process.argv[1],{flag:'wx'});fs.renameSync('/output/recovery.resume.tmp','/output/recovery.resume');"
        kube('exec','benchmark','--','node','-e',script,json.dumps(barrier))

    try:
        kube('create','-f','-',body=dict(apiVersion='v1',kind='Namespace',metadata=dict(name=namespace)))
        created = True
        report['kubernetes_version']=json.loads(kube('version','-o','json'))['serverVersion']['gitVersion']
        resources=[dict(apiVersion='v1',kind='ResourceQuota',metadata=dict(name='benchmark'),
                        spec=dict(hard={'limits.cpu':'8' if args.cold_recovery else '7','limits.memory':'3Gi','pods':'4'})),
            dict(apiVersion='v1',kind='Secret',metadata=dict(name='durable-db-storage'),stringData=storage),
            dict(apiVersion='v1',kind='Secret',metadata=dict(name='durable-db-signing'),stringData={'admin.key':Path(args.signing_key_file).read_text()}),
            dict(apiVersion='v1',kind='Secret',metadata=dict(name='benchmark-token'),stringData={'admin.token':Path(args.admin_token_file).read_text()}),
            dict(apiVersion='v1',kind='ConfigMap',metadata=dict(name='benchmark'),data={
                'benchmark.mjs':Path('scripts/benchmark-http.mjs').read_text(),
                'transport.mjs':Path('clients/javascript/transport.mjs').read_text()})]
        for resource in yaml.safe_load_all(subprocess.check_output(['kubectl','kustomize','deploy/kubernetes'])):
            if resource['kind'] not in ('StatefulSet','Service'): continue
            if resource['kind']=='StatefulSet':
                resource['spec']['replicas']=3
                spec=resource['spec']['template']['spec']
                spec['topologySpreadConstraints'][0]['whenUnsatisfiable']='DoNotSchedule'
                container=spec['containers'][0];container['image']=args.image
                container['args']=[arg for arg in container['args'] if not arg.startswith(('--disk-cache','--concurrency='))]
                container['args']+=[f'--concurrency={args.concurrency}','--maintenance-seconds=60','--upload-budget-bytes=1073741824','--upload-budget-attempts=100000']
                container['volumeMounts']=[m for m in container['volumeMounts'] if m['name']=='keys']
                spec['volumes']=[v for v in spec['volumes'] if v['name']=='keys']
                if args.cold_recovery:
                    spec['shareProcessNamespace']=True
                    spec['terminationGracePeriodSeconds']=1
                    spec['containers'].append(dict(name='fault',image=node_image,
                        command=['/bin/sleep','infinity'],workingDir='/',
                        resources=dict(requests=dict(cpu='1m',memory='8Mi'),limits=dict(cpu='100m',memory='64Mi')),
                        securityContext=dict(allowPrivilegeEscalation=False,readOnlyRootFilesystem=True,capabilities=dict(drop=['ALL']))))
                    report['configuration']['cold_recovery']=dict(process_control='same-UID SIGSTOP followed by pod termination',
                        grace_seconds=1,sidecar_image=node_image,sidecar_memory_limit='64Mi')
            resources.append(resource)
        for resource in resources:resource['metadata']['namespace']=namespace
        kube('apply','-f','-',body=dict(apiVersion='v1',kind='List',items=resources))
        kube('rollout','status','statefulset/durable-db','--timeout=240s',timeout=270)
        pods=json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
        if len({p['spec']['nodeName'] for p in pods})!=3:raise RuntimeError('Expected three distinct hosts')
        # Use the same pinned Node runtime as the fault qualifier.
        runner=dict(apiVersion='v1',kind='Pod',metadata=dict(name='benchmark'),spec=dict(
            restartPolicy='Never',automountServiceAccountToken=False,
            securityContext=dict(runAsNonRoot=True,runAsUser=1000,runAsGroup=1000,fsGroup=1000,seccompProfile=dict(type='RuntimeDefault')),
            containers=[dict(name='benchmark',image=node_image,command=['node','-e','setInterval(()=>{},1<<30)'],
                resources=dict(requests=dict(cpu='100m',memory='64Mi'),limits=dict(cpu='1',memory='256Mi')),
                securityContext=dict(allowPrivilegeEscalation=False,readOnlyRootFilesystem=True,capabilities=dict(drop=['ALL'])),
                volumeMounts=[dict(name=name,mountPath=path,readOnly=name!='output') for name,path in
                    [('scripts','/work'),('token','/run/credentials'),('output','/output')]])],
            volumes=[dict(name='scripts',configMap=dict(name='benchmark',items=[dict(key='benchmark.mjs',path='scripts/benchmark-http.mjs'),dict(key='transport.mjs',path='clients/javascript/transport.mjs')])),
                     dict(name='token',secret=dict(secretName='benchmark-token',defaultMode=0o440)),
                     dict(name='output',emptyDir=dict(sizeLimit='32Mi'))]))
        kube('apply','-f','-',body=runner)
        kube('wait','--for=condition=Ready','pod/benchmark','--timeout=120s',timeout=150)
        sample()
        report['journal_counters_start']=journal_counters();save()
        command=['node','/work/scripts/benchmark-http.mjs','--urls','http://durable-db:7654',
            '--token-file','/run/credentials/admin.token','--databases',str(args.databases),'--workers',args.workers,
            '--operations',str(args.operations),'--value-bytes',str(args.value_bytes),'--upload-budget-bytes','67108864',
            '--upload-budget-attempts','10000','--output','/output/benchmark.json',
            '--context','Three stock Kubernetes hosts; B2; native release image '+args.image+'; see resources.json for limits and counters.']
        if args.cold_recovery:command+=['--recovery-barrier','/output/recovery']
        report['command']=command;save()
        with (output/'benchmark.log').open('w') as log:
            load=subprocess.Popen(['kubectl','--context',args.context,'-n',namespace,'exec','benchmark','--',*command],stdout=log,stderr=subprocess.STDOUT)
            deadline=time.monotonic()+1800
            while load.poll() is None:
                if time.monotonic()>deadline:raise RuntimeError('Thirty-minute workload deadline exceeded')
                if args.cold_recovery and 'cold_recovery' not in report:
                    try:barrier=json.loads(kube('exec','benchmark','--','cat','/output/recovery.ready'))
                    except subprocess.CalledProcessError:barrier=None
                    if barrier:cold_restart(barrier)
                sample();time.sleep(10)
        report['benchmark_exit_code']=load.returncode
        (output/'benchmark.json').write_bytes(kube('exec','benchmark','--','cat','/output/benchmark.json'))
        if args.cold_recovery:
            if 'cold_recovery' not in report:raise RuntimeError('Cold recovery barrier was not reached')
            report['cold_recovery']['journal_counters_end']=journal_counters()
        else:report['journal_counters_end']=journal_counters()
        sample()
        report['complete']=load.returncode==0
    finally:
        if load is not None and load.poll() is None:load.terminate();load.wait(timeout=15)
        if created:
            for pod in ['durable-db-0','durable-db-1','durable-db-2']:
                try:(output/(pod+'.log')).write_bytes(kube('logs',pod,'-c','database','--tail=10000'))
                except subprocess.SubprocessError:pass
            kube('delete','namespace',namespace,'--wait=true','--timeout=180s',timeout=210)
            report['namespace_removed']=True
        report['finished_at']=now();save()
    if not report['complete']:raise SystemExit('Benchmark failed; retained partial results')


if __name__=='__main__':main()
