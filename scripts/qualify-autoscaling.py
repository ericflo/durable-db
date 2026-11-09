#!/usr/bin/env python3
"""Destructive qualification confined to an explicitly named disposable kind cluster.

Requires a provisioned namespace, signing/storage Secrets, metrics-server, a
loaded image, and a NodePort endpoint. Only synthetic data is used. The caller
owns the cluster and provider cleanup. Reports retain failed phases and retries;
they contain aggregate counts and hashes, never credentials or stored values.
"""
import argparse
import base64
import datetime
import hashlib
import ipaddress
import json
from pathlib import Path
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid

import yaml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kubeconfig', required=True)
    parser.add_argument('--kind-cluster', required=True)
    parser.add_argument('--namespace', default='durable-db-qualification')
    parser.add_argument('--image', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--token-file', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--context', required=True)
    parser.add_argument('--provider-ip', help='Optional disposable provider IPv4 for a 40-second node egress partition')
    parser.add_argument('--cpu-request', default='100m')
    parser.add_argument('--read-workers', type=int, default=32)
    parser.add_argument('--phase-timeout', type=int, default=600)
    parser.add_argument('--disposable', action='store_true', required=True)
    args = parser.parse_args()
    if not args.kind_cluster.startswith('durable-db-qualification') or not args.namespace.startswith('durable-db-qualification'):
        parser.error('Both cluster and namespace must start with durable-db-qualification')
    if args.read_workers not in range(1, 129) or args.phase_timeout not in range(60, 1801):
        parser.error('Read workers must be 1..128 and phase timeout 60..1800 seconds')
    if args.provider_ip:
        address = ipaddress.ip_address(args.provider_ip)
        if address.version != 4 or not address.is_private or address.is_loopback:
            parser.error('Partition target must be a private, non-loopback IPv4 address')
    token = Path(args.token_file).read_text().strip()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    report = dict(schema=1, complete=False, started_at=utc(), context=args.context,
                  source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                  configuration=dict(image=args.image, cpu_request=args.cpu_request, cpu_limit='1',
                                     cpu_target_percent=70, read_workers=args.read_workers,
                                     maintenance_seconds=15, memory_cache_mib=64, disk_cache_mib=128,
                                     hpa_down_stabilization_seconds=30, hpa_down_period_seconds=15),
                  harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  phases=[], observations=[], http_attempt_statuses={}, writes_acknowledged=0,
                  reads_verified=0, failures=[], limitations=[
                      'Disposable kind cluster on one physical host, not independent infrastructure failure domains.',
                      'CPU requests and HPA downscale windows are qualification settings, not capacity recommendations.',
                      'Single sequential append writer per database; no cross-client contested-register linearizability claim.',
                      'Short fault campaign; no sustained-load, AWS, or production SLO claim.'])
    writer_stop, reader_stop = threading.Event(), threading.Event()
    writer_threads, reader_threads = [], []
    expected, credentials = {}, {}
    blocked_node = None
    drained_node = None

    def kube(*command, body=None):
        return subprocess.check_output(['kubectl', '--kubeconfig', args.kubeconfig, '-n', args.namespace, *command],
                                       input=None if body is None else json.dumps(body).encode(), stderr=subprocess.PIPE, timeout=60)

    def docker(*command):
        return subprocess.check_output(['docker', *command], stderr=subprocess.PIPE, timeout=60)

    def save():
        with lock:
            output.write_text(json.dumps(report, indent=2) + '\n')

    def phase(name, **details):
        with lock:
            report['phases'].append(dict(name=name, time_utc=utc(), writes_acknowledged=report['writes_acknowledged'], **details))
        save()
        print(json.dumps(report['phases'][-1]), flush=True)

    def assert_workers():
        with lock:
            if report['failures']:
                raise RuntimeError('A workload worker failed; see sanitized report')

    def rpc(database, suffix='', body=None, method=None, admin=False, timeout=240, stop=None):
        encoded = None if body is None else json.dumps(body).encode()
        path = '/v1/tokens' if suffix == '/tokens' else '/v1/databases/' + database + suffix
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            if stop and stop.is_set():
                return None
            request = urllib.request.Request(args.url.rstrip('/') + path, data=encoded, method=method,
                                             headers={'Authorization': 'Bearer ' + (token if admin else credentials[database]),
                                                      'Content-Type': 'application/json'})
            try:
                with urllib.request.urlopen(request, timeout=35) as response:
                    result = json.load(response)
                    status = response.status
            except urllib.error.HTTPError as error:
                status = error.code
                error.close()
                if status not in (409, 429, 500, 502, 503, 504):
                    raise RuntimeError(f'Unexpected HTTP status {status}') from None
                result = None
            except (OSError, ValueError):
                status, result = 'transport_or_decode_error', None
            with lock:
                counts = report['http_attempt_statuses']
                counts[str(status)] = counts.get(str(status), 0) + 1
            if result is not None:
                return result
            if time.monotonic() >= deadline:
                raise RuntimeError('Identical-request recovery budget exhausted')
            attempt += 1
            delay = min(.05 * attempt, 1)
            if stop:
                stop.wait(delay)
            else:
                time.sleep(delay)

    def pods():
        return json.loads(kube('get', 'pods', '-l', 'app=durable-db', '-o', 'json'))['items']

    def wait_replicas(count, *, cpu=False):
        deadline = time.monotonic() + args.phase_timeout
        while time.monotonic() < deadline:
            assert_workers()
            deployment = json.loads(kube('get', 'deployment', 'durable-db', '-o', 'json'))
            hpa = json.loads(kube('get', 'hpa', 'durable-db', '-o', 'json'))
            live = pods()
            ready = [p for p in live if not p['metadata'].get('deletionTimestamp') and
                     p['status'].get('containerStatuses') and all(c.get('ready') for c in p['status']['containerStatuses'])]
            observation = dict(time_utc=utc(), wanted=count, requested=deployment['spec']['replicas'], ready=len(ready),
                               hpa_current=hpa.get('status', {}).get('currentReplicas'),
                               hpa_desired=hpa.get('status', {}).get('desiredReplicas'),
                               current_metrics=hpa.get('status', {}).get('currentMetrics', []),
                               conditions=[{k: c.get(k) for k in ('type', 'status', 'reason')} for c in hpa.get('status', {}).get('conditions', [])])
            with lock:
                report['observations'].append(observation)
            save()
            metric_ready = not cpu or any(c.get('type') == 'ScalingActive' and c.get('status') == 'True' for c in hpa.get('status', {}).get('conditions', []))
            target = deployment['spec']['replicas'] if count is None else count
            if target >= 1 and deployment['spec']['replicas'] == target and len(ready) == target and metric_ready:
                return observation
            time.sleep(5)
        raise RuntimeError(f'Replica target {count} did not converge')

    def create(database, load=False):
        rpc(database, body=dict(request_id=str(uuid.uuid4()), limits=dict(max_bytes=16 * 1024 * 1024, max_keys=100000)), method='PUT', admin=True)
        credentials[database] = rpc(database, '/tokens', dict(database=database, write=True, ttl_seconds=14400), admin=True)['token']
        expected[database] = 0
        if load:
            value = base64.b64encode(b'x' * 32768).decode()
            rpc(database, '/commit', dict(request_id=str(uuid.uuid4()), expected_revision=0,
                                         writes=[dict(key=f'load-{i:02}', value=value) for i in range(16)]))

    def writer(database, index):
        try:
            while not writer_stop.is_set():
                revision = expected[database]
                value = base64.b64encode(f'{index}/{revision}'.encode()).decode()
                request_id = str(uuid.uuid4())
                body = dict(request_id=request_id, expected_revision=revision,
                            writes=[dict(key=f'{revision:08}', value=value)])
                started = time.monotonic()
                # No stop argument: an issued mutation must resolve, even while
                # the controller is stopping the next workload invocation.
                result = rpc(database, '/commit', body)
                if result['revision'] != revision + 1:
                    raise RuntimeError('Acknowledged revision mismatch')
                expected[database] = revision + 1
                with lock:
                    report['writes_acknowledged'] += 1
                    history.write(json.dumps(dict(database_index=index, request_id=request_id, revision=revision + 1,
                                                  start=started, end=time.monotonic(), value_sha256=hashlib.sha256(base64.b64decode(value)).hexdigest())) + '\n')
                    history.flush()
                # A read invoked after acknowledgement must include this write.
                read = rpc(database, '/read', dict(keys=[f'{revision:08}']))
                if read['revision'] != revision + 1 or read['values'] != [value]:
                    raise RuntimeError('Read-after-acknowledgement mismatch')
                with lock:
                    report['reads_verified'] += 1
                writer_stop.wait(.25)
        except Exception as error:
            with lock:
                report['failures'].append(dict(worker='writer', database_index=index, error_type=type(error).__name__, reason=str(error)))

    def reader(database, index):
        try:
            while not reader_stop.is_set():
                read = rpc(database, '/read', dict(scans=[dict(limit=16)]), stop=reader_stop)
                if read is None:
                    return
                items = read['scans'][0]['items']
                if read['revision'] != 1 or len(items) != 16 or any(
                        item['key'] != f'load-{i:02}' or base64.b64decode(item['value']) != b'x' * 32768
                        for i, item in enumerate(items)):
                    raise RuntimeError('Load read contents mismatch')
                with lock:
                    report['reads_verified'] += 1
        except Exception as error:
            with lock:
                report['failures'].append(dict(worker='reader', worker_index=index, error_type=type(error).__name__, reason=str(error)))

    def stop_threads(event, threads):
        event.set()
        deadline = time.monotonic() + 280
        for thread in threads:
            thread.join(max(0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError('Workload did not resolve within shutdown budget')

    def verify_all(databases):
        verified = 0
        for index, database in enumerate(databases):
            revision, cursor, count = expected[database], None, 0
            while True:
                scan = dict(limit=500)
                if cursor is not None:
                    scan['after'] = cursor
                result = rpc(database, '/read', dict(expected_revision=revision, scans=[scan]))
                for item in result['scans'][0]['items']:
                    if item['key'] != f'{count:08}' or base64.b64decode(item['value']) != f'{index}/{count}'.encode():
                        raise RuntimeError('Cold verification key/value mismatch')
                    count += 1
                cursor = result['scans'][0]['next']
                if cursor is None:
                    break
            if count != revision:
                raise RuntimeError('Cold verification revision/count mismatch')
            verified += count
        report['verified_keys'] = verified

    cluster_nodes = json.loads(kube('get', 'nodes', '-o', 'json'))['items']
    if len(cluster_nodes) != 3 or any(not n['metadata']['name'].startswith(args.kind_cluster + '-') for n in cluster_nodes):
        raise RuntimeError('Expected exactly three nodes in the named disposable kind cluster')
    context = kube('config', 'current-context').decode().strip()
    if context != 'kind-' + args.kind_cluster:
        raise RuntimeError('Explicit kubeconfig context does not match the disposable cluster')
    for node in cluster_nodes:
        label = docker('inspect', node['metadata']['name'], '--format', '{{index .Config.Labels "io.x-k8s.kind.cluster"}}').decode().strip()
        if label != args.kind_cluster:
            raise RuntimeError('Node is not a Docker container belonging to the named kind cluster')
    report['kubernetes_version'] = json.loads(kube('version', '-o', 'json'))['serverVersion']['gitVersion']
    report['nodes'] = [n['metadata']['name'] for n in cluster_nodes]
    base = subprocess.check_output(['kubectl', 'kustomize', 'deploy/kubernetes'], text=True)
    resources, hpa_resource = [], None
    for resource in yaml.safe_load_all(base):
        if resource['kind'] == 'Namespace':
            continue
        resource['metadata']['namespace'] = args.namespace
        if resource['kind'] == 'Deployment':
            container = resource['spec']['template']['spec']['containers'][0]
            container['image'] = args.image
            container['resources']['requests']['cpu'] = args.cpu_request
            container['resources']['limits']['cpu'] = '1'
            container['args'].append('--maintenance-seconds=15')
        if resource['kind'] == 'HorizontalPodAutoscaler':
            resource['spec']['maxReplicas'] = 1
            resource['spec']['behavior']['scaleDown'] = dict(stabilizationWindowSeconds=30, policies=[dict(type='Pods', value=8, periodSeconds=15)])
            hpa_resource = resource
        if resource['kind'] == 'Service':
            resource['spec']['type'] = 'NodePort'
            resource['spec']['ports'][0]['nodePort'] = 30080
        resources.append(resource)
    history_path = output.with_suffix('.history.jsonl')
    with history_path.open('w') as history:
        try:
            kube('apply', '-f', '-', body=dict(apiVersion='v1', kind='List', items=resources))
            phase('initial-one', observed=wait_replicas(1))
            run_id = uuid.uuid4().hex[:12]
            report['synthetic_run_id'] = run_id
            save()
            writer_dbs = [f'hpa-{run_id}-writer-{i}' for i in range(4)]
            read_dbs = [f'hpa-{run_id}-reader-{i}' for i in range(8)]
            for database in writer_dbs:
                create(database)
            for database in read_dbs:
                create(database, load=True)
            for index, database in enumerate(writer_dbs):
                thread = threading.Thread(target=writer, args=(database, index), daemon=True)
                writer_threads.append(thread)
                thread.start()
            for index in range(args.read_workers):
                thread = threading.Thread(target=reader, args=(read_dbs[index % len(read_dbs)], index), daemon=True)
                reader_threads.append(thread)
                thread.start()
            kube('patch', 'hpa', 'durable-db', '--type=merge', '-p', json.dumps(dict(spec=dict(maxReplicas=3))))
            phase('cpu-scale-three', observed=wait_replicas(3, cpu=True))
            kube('patch', 'hpa', 'durable-db', '--type=merge', '-p', json.dumps(dict(spec=dict(maxReplicas=8))))
            phase('cpu-scale-eight', observed=wait_replicas(8, cpu=True))

            chosen = next(p for p in pods() if not p['metadata'].get('deletionTimestamp') and p['status'].get('phase') == 'Running')
            drained_node = chosen['spec']['nodeName']
            kube('drain', drained_node, '--ignore-daemonsets', '--delete-emptydir-data', '--timeout=50s')
            # After demonstrating CPU-driven eight replicas, let the HPA keep
            # reacting during faults. Requiring exactly eight under every load
            # fluctuation would mistake a valid downscale for failed recovery.
            phase('node-drained', node=drained_node, observed=wait_replicas(None, cpu=True))
            kube('uncordon', drained_node)
            drained_node = None

            chosen = next(p for p in pods() if not p['metadata'].get('deletionTimestamp') and p['status'].get('phase') == 'Running')
            container_id = chosen['status']['containerStatuses'][0]['containerID'].split('://', 1)[1]
            node = chosen['spec']['nodeName']
            inspected = json.loads(docker('exec', node, 'crictl', 'inspect', container_id))
            if inspected['status']['labels'].get('io.kubernetes.pod.uid') != chosen['metadata']['uid']:
                raise RuntimeError('Container identity changed before SIGKILL')
            pid = int(inspected['info']['pid'])
            if pid <= 1:
                raise RuntimeError('Unsafe container PID')
            docker('exec', node, 'kill', '-KILL', str(pid))
            time.sleep(15)
            phase('api-process-sigkill', node=node, observed=wait_replicas(None, cpu=True))

            if args.provider_ip:
                blocked_node = node
                rule = ['FORWARD', '-d', args.provider_ip, '-p', 'tcp', '--dport', '9000', '-m', 'comment', '--comment', 'durable-db-qualification', '-j', 'DROP']
                docker('exec', blocked_node, 'iptables', '-I', *rule)
                phase('provider-partition-start', node=blocked_node, duration_seconds=40)
                time.sleep(40)
                counters = docker('exec', blocked_node, 'iptables', '-L', 'FORWARD', '-n', '-v', '-x').decode()
                matching = [line.split() for line in counters.splitlines() if 'durable-db-qualification' in line]
                if len(matching) != 1 or int(matching[0][0]) == 0:
                    raise RuntimeError('Partition rule did not intercept provider packets')
                report['partition_dropped_packets'] = int(matching[0][0])
                docker('exec', blocked_node, 'iptables', '-D', *rule)
                blocked_node = None
                phase('provider-partition-healed', observed=wait_replicas(None, cpu=True))

            stop_threads(reader_stop, reader_threads)
            stop_threads(writer_stop, writer_threads)
            assert_workers()
            # minReplicas remains one; only removal of CPU load should downscale.
            phase('cpu-scale-one', observed=wait_replicas(1, cpu=True))
            verify_all(writer_dbs)
            kube('delete', 'hpa', 'durable-db')
            kube('scale', 'deployment', 'durable-db', '--replicas=0')
            deadline = time.monotonic() + 90
            while pods():
                if time.monotonic() > deadline:
                    raise RuntimeError('API pods did not disappear at zero replicas')
                time.sleep(2)
            phase('zero-api-replicas', verified_keys=report['verified_keys'])
            kube('scale', 'deployment', 'durable-db', '--replicas=1')
            kube('apply', '-f', '-', body=hpa_resource)
            phase('cold-one', observed=wait_replicas(1))
            verify_all(writer_dbs)
            if report['verified_keys'] != report['writes_acknowledged']:
                raise RuntimeError('Acknowledged-write total differs from cold recovered keys')
            report['complete'] = True
        except BaseException as error:
            report['controller_failure'] = dict(type=type(error).__name__, reason=str(error))
            raise
        finally:
            if blocked_node:
                docker('exec', blocked_node, 'iptables', '-D', *rule)
            if drained_node:
                kube('uncordon', drained_node)
            stop_threads(reader_stop, reader_threads)
            stop_threads(writer_stop, writer_threads)
            report['finished_at'] = utc()
            report['history_sha256'] = hashlib.sha256(history_path.read_bytes()).hexdigest()
            save()
    print(json.dumps(dict(complete=report['complete'], writes_acknowledged=report['writes_acknowledged'], verified_keys=report['verified_keys'])), flush=True)


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


if __name__ == '__main__':
    main()
