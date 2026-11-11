#!/usr/bin/env python3
"""Update generated Pages files without replacing independent site edits."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

MANIFEST = '.durable-db-generated.json'
MARKER = 'deployment.json'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def merge(source, site, commit):
    manifest = site / MANIFEST
    if manifest.exists():
        previous = json.loads(manifest.read_text())
    else:
        # Bootstrap from the last whole-site publisher, not today's site tree.
        base = subprocess.check_output([
            'git', '-C', str(site), 'log', '-1', '--format=%H',
            '--grep=^Publish docs for [0-9a-f]',
        ], text=True).strip()
        previous = {}
        if base:
            paths = subprocess.check_output([
                'git', '-C', str(site), 'ls-tree', '-r', '--name-only', base,
            ], text=True).splitlines()
            for name in paths:
                if name not in (MANIFEST, MARKER):
                    previous[name] = digest(subprocess.check_output([
                        'git', '-C', str(site), 'show', base + ':' + name,
                    ]))
    generated = {str(p.relative_to(source)): digest(p.read_bytes())
                 for p in source.rglob('*') if p.is_file()}
    preserved = []
    for name in sorted(previous.keys() | generated.keys()):
        relative = Path(name)
        if relative.is_absolute() or '..' in relative.parts or '.git' in relative.parts:
            raise ValueError('invalid generated path')
        target = site / relative
        if any(p.is_symlink() for p in [target, *target.parents] if p != site.parent):
            preserved.append(name)
            continue
        current = digest(target.read_bytes()) if target.is_file() else None
        if current != previous.get(name) and current != generated.get(name):
            preserved.append(name)
            continue
        if current is None and name in previous:
            preserved.append(name)  # An independent deletion also belongs to its author.
            continue
        if name in generated:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((source / name).read_bytes())
        elif target.is_file():
            target.unlink()  # Remove obsolete generated files only if still unedited.
    manifest.write_text(json.dumps(generated, sort_keys=True) + '\n')
    (site / MARKER).write_text(json.dumps({
        'source_commit': commit, 'preserved_overrides': preserved,
    }) + '\n')
    for name in preserved:
        print('Preserved independent site change: ' + name)
    return preserved


if __name__ == '__main__':
    merge(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
