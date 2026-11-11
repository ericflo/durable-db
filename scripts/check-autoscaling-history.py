#!/usr/bin/env python3
"""Check sequential append histories from the Kubernetes qualification harness.

This checks the fixture's identities, invocation order, hashes and recovered
counts. It is not a general concurrent-object linearizability checker.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    history = args.report.with_suffix('.history.jsonl')
    raw = history.read_bytes()
    if not report.get('complete') or report.get('failures') or report.get('controller_failure'):
        raise ValueError('Campaign did not complete successfully')
    if hashlib.sha256(raw).hexdigest() != report['history_sha256']:
        raise ValueError('History checksum mismatch')
    records = [json.loads(line) for line in raw.splitlines()]
    if len(records) != report['writes_acknowledged'] or len(records) != report['verified_keys']:
        raise ValueError('Acknowledged, recorded and recovered totals differ')
    identities, previous = set(), {}
    for record in records:
        identity = str(uuid.UUID(record['request_id']))
        if identity != record['request_id'] or identity in identities:
            raise ValueError('Repeated or invalid request identity')
        identities.add(identity)
        database, revision = record['database_index'], record['revision']
        prior = previous.get(database, dict(revision=0, end=0))
        if database not in range(4) or revision != prior['revision'] + 1:
            raise ValueError('Unexpected database or non-contiguous revision')
        start, end = record['start'], record['end']
        if not all(math.isfinite(v) for v in (start, end)) or start < prior['end'] or end < start:
            raise ValueError('Invalid per-writer invocation order')
        expected = hashlib.sha256(f'{database}/{revision - 1}'.encode()).hexdigest()
        if record['value_sha256'] != expected:
            raise ValueError('Recorded value hash does not match the fixture')
        previous[database] = record
    print(json.dumps(dict(complete=True, histories=len(previous), acknowledged=len(records),
                          check='sequential append identity, invocation order, value hashes and recovered totals')))


if __name__ == '__main__':
    main()
