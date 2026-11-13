#!/usr/bin/env python3
"""Check sequential append histories from the Kubernetes qualification harness.

This checks the fixture's identities, invocation order, hashes and recovered
counts. Schema 2 also checks verified readback times and fault-window gaps on
the workload's single monotonic clock. It is not a general linearizability checker.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import uuid


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def readback_windows(records, windows):
    times = {writer: [] for writer in range(4)}
    for record in records:
        times[record['database_index']].append(record['readback_end'])
    result, names, previous_end = [], set(), 0
    for window in windows:
        name, start, end = window['name'], window['start'], window['end']
        target = window['target_seconds']
        if name in names or not finite(target) or target <= 0:
            raise ValueError('Invalid readback window identity or target')
        names.add(name)
        for point in (start, end):
            if not finite(point['seconds']) or len(point['verified']) != 4:
                raise ValueError('Invalid workload clock or writer counts')
            for writer, count in enumerate(point['verified']):
                history = times[writer]
                if type(count) is not int or not 1 <= count <= len(history):
                    raise ValueError('Window requires prior verified progress from every writer')
                # Counts disambiguate completions sharing a clock tick.
                if history[count-1] > point['seconds'] or (count < len(history) and history[count] < point['seconds']):
                    raise ValueError('Window counters disagree with readback timestamps')
        if not previous_end <= start['seconds'] <= end['seconds']:
            raise ValueError('Reversed or overlapping readback windows')
        previous_end = end['seconds']
        gaps, tails, counts = [], [], []
        for writer in range(4):
            before, after = start['verified'][writer], end['verified'][writer]
            if after < before + 2:
                raise ValueError('Window lacks its per-writer readback barrier')
            history = times[writer]
            gaps.append(max(history[i]-history[i-1] for i in range(before, after)))
            tails.append(end['seconds']-history[after-1])
            counts.append(after-before)
        result.append(dict(name=name,duration_seconds=end['seconds']-start['seconds'],
                           readbacks=counts,max_completed_readback_gap_seconds=gaps,
                           open_readback_gap_seconds=tails,target_seconds=target,
                           observed_within_target=max(gaps+tails)<=target))
    return result


def check(report, raw):
    schema = report.get('schema', 1)
    if schema not in (1, 2):
        raise ValueError('Unsupported qualification schema')
    if not report.get('complete') or report.get('failures') or report.get('controller_failure'):
        raise ValueError('Campaign did not complete successfully')
    if hashlib.sha256(raw).hexdigest() != report['history_sha256']:
        raise ValueError('History checksum mismatch')
    records = [json.loads(line) for line in raw.splitlines()]
    if len(records) != report['writes_acknowledged'] or len(records) != report['verified_keys']:
        raise ValueError('Acknowledged, recorded and recovered totals differ')
    if schema == 2 and report['reads_verified'] != len(records):
        raise ValueError('Unverified acknowledged writes remain')
    identities, previous = set(), {}
    for record in records:
        identity = str(uuid.UUID(record['request_id']))
        if identity != record['request_id'] or identity in identities:
            raise ValueError('Repeated or invalid request identity')
        identities.add(identity)
        database, revision = record['database_index'], record['revision']
        prior = previous.get(database, dict(revision=0, end=0, readback_end=0))
        if database not in range(4) or revision != prior['revision'] + 1:
            raise ValueError('Unexpected database or non-contiguous revision')
        start, end = record['start'], record['end']
        if not all(finite(v) for v in (start, end)) or start < prior['end'] or end < start:
            raise ValueError('Invalid per-writer invocation order')
        if schema == 2:
            readback = record['readback_end']
            if not finite(readback) or readback < end or start < prior['readback_end']:
                raise ValueError('Invalid verified readback order')
        expected = hashlib.sha256(f'{database}/{revision - 1}'.encode()).hexdigest()
        if record['value_sha256'] != expected:
            raise ValueError('Recorded value hash does not match the fixture')
        previous[database] = record
    result = dict(complete=True, histories=len(previous), acknowledged=len(records),
                  check='sequential append identity, invocation order, value hashes and recovered totals')
    if schema == 2:
        if len(previous) != 4:
            raise ValueError('Missing writer history')
        expected = [p.get('readback_window',p['name']) for p in report['phases']
                    if 'readback_window' in p or p['name'].startswith('rolling-replacement')]
        if [w['name'] for w in report.get('readback_windows', [])] != expected:
            raise ValueError('Missing or unexpected readback window')
        result['readback_windows'] = readback_windows(records, report.get('readback_windows', []))
        result['readback_scope'] = ('Workload monotonic clock only. Completed gaps include the last readback before window start; '
                                    'the open gap ends at the closing observation and is right-censored. '
                                    'No cross-host clock comparison, attribution of all delay to the fault, or production SLO.')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    print(json.dumps(check(json.loads(args.report.read_text()), args.report.with_suffix('.history.jsonl').read_bytes())))


if __name__ == '__main__':
    main()
