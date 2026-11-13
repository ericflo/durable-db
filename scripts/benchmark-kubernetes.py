#!/usr/bin/env python3
"""Run benchmark-http on three pods in a fresh disposable namespace.

Reuses deploy/kubernetes and the SDK benchmark; samples only these database
containers. Caller supplies a dedicated bucket and owns provider cleanup.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time
import uuid

import yaml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['context', 'image', 'storage-env-file', 'signing-key-file', 'admin-token-file', 'output']:
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--disposable', action='store_true', required=True)
    args = parser.parse_args()
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
                  configuration=dict(replicas=3, partitions=16, concurrency_per_process=8,
                      memory_cache_mib=64, resident_memory_mib=128, disk_cache=False,
                      cpu_limit='2', memory_limit='768Mi', maintenance_seconds=60,
                      namespace_upload_budget=dict(bytes=1073741824,attempts=100000),
                      resource_sample_interval_seconds=10), resource_samples=[])
    created = False
    load = None

    def kube(*cmd, body=None, timeout=60):
        return subprocess.check_output(['kubectl','--context',args.context,'-n',namespace,*cmd],
            input=None if body is None else json.dumps(body).encode(), stderr=subprocess.PIPE, timeout=timeout)

    def save():
        (output/'resources.json').write_text(json.dumps(report,indent=2)+'\n')

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
        report['resource_samples'].append(dict(time_utc=now(),pods=observed)); save()
        if len(observed)!=3 or any(p['rss_bytes'] is None or p['cpu_usage_nanoseconds'] is None or p['restart_count'] for p in observed):
            raise RuntimeError('Incomplete counters or an unplanned database restart')
        if {p['uid'] for p in observed}!={p['uid'] for p in report['resource_samples'][0]['pods']}:
            raise RuntimeError('Database pod replaced during baseline')

    try:
        kube('create','-f','-',body=dict(apiVersion='v1',kind='Namespace',metadata=dict(name=namespace)))
        created = True
        report['kubernetes_version']=json.loads(kube('version','-o','json'))['serverVersion']['gitVersion']
        resources=[dict(apiVersion='v1',kind='ResourceQuota',metadata=dict(name='benchmark'),
                        spec=dict(hard={'limits.cpu':'7','limits.memory':'3Gi','pods':'4'})),
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
                container['args']=[arg for arg in container['args'] if not arg.startswith('--disk-cache')]
                container['args']+=['--maintenance-seconds=60','--upload-budget-bytes=1073741824','--upload-budget-attempts=100000']
                container['volumeMounts']=[m for m in container['volumeMounts'] if m['name']=='keys']
                spec['volumes']=[v for v in spec['volumes'] if v['name']=='keys']
            resources.append(resource)
        for resource in resources:resource['metadata']['namespace']=namespace
        kube('apply','-f','-',body=dict(apiVersion='v1',kind='List',items=resources))
        kube('rollout','status','statefulset/durable-db','--timeout=240s',timeout=270)
        pods=json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
        if len({p['spec']['nodeName'] for p in pods})!=3:raise RuntimeError('Expected three distinct hosts')
        # Use the same pinned Node runtime as the fault qualifier.
        import runpy
        node_image=runpy.run_path('scripts/qualify-multinode.py')['NODE_IMAGE']
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
        command=['node','/work/scripts/benchmark-http.mjs','--urls','http://durable-db:7654',
            '--token-file','/run/credentials/admin.token','--databases','32','--workers','1,4,16,32',
            '--operations','20','--value-bytes','1024','--upload-budget-bytes','67108864',
            '--upload-budget-attempts','10000','--output','/output/benchmark.json',
            '--context','Three stock Kubernetes hosts; B2; native release image '+args.image+'; see resources.json for limits and counters.']
        report['command']=command;save()
        with (output/'benchmark.log').open('w') as log:
            load=subprocess.Popen(['kubectl','--context',args.context,'-n',namespace,'exec','benchmark','--',*command],stdout=log,stderr=subprocess.STDOUT)
            deadline=time.monotonic()+1800
            while load.poll() is None:
                if time.monotonic()>deadline:raise RuntimeError('Thirty-minute workload deadline exceeded')
                sample();time.sleep(10)
        report['benchmark_exit_code']=load.returncode
        (output/'benchmark.json').write_bytes(kube('exec','benchmark','--','cat','/output/benchmark.json'))
        sample()
        report['complete']=load.returncode==0
    finally:
        if load is not None and load.poll() is None:load.terminate();load.wait(timeout=15)
        if created:
            for pod in ['durable-db-0','durable-db-1','durable-db-2']:
                try:(output/(pod+'.log')).write_bytes(kube('logs',pod,'--tail=10000'))
                except subprocess.SubprocessError:pass
            kube('delete','namespace',namespace,'--wait=true','--timeout=180s',timeout=210)
            report['namespace_removed']=True
        report['finished_at']=now();save()
    if not report['complete']:raise SystemExit('Benchmark failed; retained partial results')


if __name__=='__main__':main()
