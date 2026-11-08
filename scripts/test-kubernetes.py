#!/usr/bin/env python3
"""Opt-in qualification in an ALREADY PROVISIONED disposable namespace.
Requires PyYAML, kubectl, DURABLE_DB_TEST_IMAGE, DURABLE_DB_TEST_TOKEN_FILE.
The namespace must start with durable-db-qualification. Provision its signing
and provider Secrets first. No operator/provider credentials enter this script.
"""
import json, os, pathlib, subprocess, time, urllib.error, urllib.request, uuid
import yaml
namespace=os.environ.get('DURABLE_DB_TEST_NAMESPACE','durable-db-qualification')
if not namespace.startswith('durable-db-qualification'):
    raise SystemExit('Use a dedicated durable-db-qualification namespace')
image=os.environ['DURABLE_DB_TEST_IMAGE']
token=pathlib.Path(os.environ['DURABLE_DB_TEST_TOKEN_FILE']).read_text().strip()
database='k8s-'+uuid.uuid4().hex
forwards=[]
records=[]
def kube(*args,body=None):
    return subprocess.check_output(['kubectl','-n',namespace,*args],input=None if body is None else json.dumps(body).encode(),stderr=subprocess.PIPE)
def wait_replicas(count):
    deadline=time.monotonic()+240
    while time.monotonic()<deadline:
        d=json.loads(kube('get','deployment','durable-db','-o','json'))
        if d['spec']['replicas']==count and d['status'].get('readyReplicas',0)==count:
            pods=json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
            ready=[p for p in pods if not p['metadata'].get('deletionTimestamp') and p['status'].get('phase')=='Running' and p['status'].get('containerStatuses') and all(c.get('ready') for c in p['status']['containerStatuses'])]
            if len(ready)==count:return
        time.sleep(2)
    raise RuntimeError('HPA/ready replica count failed to converge')
def rpc(port,path,body=None):
    request=urllib.request.Request(f'http://127.0.0.1:{port}{path}',data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    for attempt in range(10):
        try:
            with urllib.request.urlopen(request,timeout=35) as response:
                data=response.read();return json.loads(data) if data else None
        except urllib.error.HTTPError as e:
            if e.code not in (429,503) or attempt==9:raise
            time.sleep(.2*(attempt+1))
def close_forwards():
    for p in forwards:p.terminate()
    for p in forwards:p.wait(timeout=10)
    forwards.clear()
def connect_pods():
    close_forwards()
    pods=json.loads(kube('get','pods','-l','app=durable-db','-o','json'))['items']
    pods=[p for p in pods if not p['metadata'].get('deletionTimestamp') and p['status'].get('phase')=='Running' and all(c.get('ready') for c in p['status'].get('containerStatuses',[]))]
    ports=[]
    for n,pod in enumerate(pods):
        port=18651+n
        p=subprocess.Popen(['kubectl','-n',namespace,'port-forward','pod/'+pod['metadata']['name'],f'{port}:7654'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        forwards.append(p)
        for _ in range(100):
            if p.poll() is not None:raise RuntimeError('port forward exited')
            try:rpc(port,'/health/ready');break
            except (OSError,urllib.error.URLError):time.sleep(.1)
        else:raise RuntimeError('port forward not ready')
        ports.append(port)
    return ports
base=subprocess.check_output(['kubectl','kustomize','deploy/kubernetes'],text=True)
resources=[]
for resource in yaml.safe_load_all(base):
    if resource['kind']=='Namespace':continue
    resource['metadata']['namespace']=namespace
    if resource['kind']=='Deployment':resource['spec']['template']['spec']['containers'][0]['image']=image
    if resource['kind']=='HorizontalPodAutoscaler':
        resource['spec']['maxReplicas']=1
        resource['spec']['behavior']['scaleDown']['stabilizationWindowSeconds']=0
    resources.append(resource)
kube('apply','-f','-',body={'apiVersion':'v1','kind':'List','items':resources})
expected={}
revision=0
try:
    for replicas in (1,3,1):
        kube('patch','hpa','durable-db','--type=merge','-p',json.dumps({'spec':{'minReplicas':replicas,'maxReplicas':replicas}}))
        wait_replicas(replicas)
        ports=connect_pods()
        assert len(ports)==replicas
        if not expected:rpc(ports[0],f'/v1/databases/{database}',{'max_bytes':1048576,'max_keys':1000})
        for _ in range(12):
            key=f'k{revision:04}'
            body={'expected_revision':revision,'request_id':str(uuid.uuid4()),'writes':[{'key':key,'value':'b2s='}]}
            for attempt in range(50):
                try:result=rpc(ports[revision%len(ports)],f'/v1/databases/{database}/commit',body);break
                except urllib.error.HTTPError as e:
                    if e.code!=409:raise
                    time.sleep(.05)
            else:raise RuntimeError('write contention did not settle')
            revision=result['revision'];expected[key]='b2s='
        hpa=json.loads(kube('get','hpa','durable-db','-o','json'))
        record={'replicas':replicas,'revision':revision,'hpa_conditions':[{k:c.get(k) for k in ('type','status','reason')} for c in hpa.get('status',{}).get('conditions',[])],'time_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
        records.append(record);print(json.dumps(record),flush=True)
    # Force-remove the last pod: the Deployment must restore service from B2/S3 alone.
    close_forwards()
    kube('delete','pods','-l','app=durable-db','--grace-period=0','--force','--wait=false')
    wait_replicas(1)
    ports=connect_pods()
    result=rpc(ports[0],f'/v1/databases/{database}/read',{'scans':[{'limit':100}]})
    actual={p['key']:p['value'] for p in result['scans'][0]['items']}
    assert result['revision']==36 and actual==expected,(result['revision'],len(actual))
    report={'image':image,'hpa_phases':records,'forced_pod_recovery':True,'verified_revision':36,'verified_keys':len(actual),'scope':'HPA bound changes 1→3→1; force deletion; real provider; not CPU-load trigger or node-partition qualification'}
    output=pathlib.Path(os.environ.get('DURABLE_DB_TEST_REPORT','.tools/kubernetes-result.json'))
    output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
    print('Kubernetes HPA and forced pod recovery passed; all 36 writes verified.',flush=True)
finally:
    close_forwards()
