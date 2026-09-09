"""Verify exact source members and active qualified versions; no mutation or payload logging."""
from __future__ import annotations

import ast
import hashlib
import io
import json
import re
import subprocess
import urllib.request
import zipfile
from pathlib import Path


def release_config(root, function):
    path = root / 'aws/lambdas' / function / 'config.json'
    return json.loads(path.read_text()) if path.exists() else {}


def shared_imports(root, sources):
    shared = {p.stem: p for p in (root / 'aws/shared').glob('*.py')}
    needed = set()
    queue = list(sources)
    while queue:
        path = queue.pop()
        if path.suffix != '.py':
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names = [node.module.split('.')[0]] if isinstance(node, ast.ImportFrom) and node.module else [a.name.split('.')[0] for a in node.names] if isinstance(node, ast.Import) else []
            for name in names:
                if name in shared and name not in needed:
                    needed.add(name)
                    queue.append(shared[name])
    return [shared[name] for name in sorted(needed)]


def check_packages(lam, root, functions):
    result = []
    for function in functions:
        config = release_config(root, function)
        qualifier = 'live' if config.get('release_validation') or function in ('justhodl-engine-fusion', 'justhodl-khalid-risk') else None
        request = {'FunctionName': function}
        if qualifier: request['Qualifier'] = qualifier
        row = {'function': function, 'qualifier': qualifier or '$LATEST', 'files': [], 'pass': False}
        try:
            deployed = lam.get_function(**request)
            state = deployed['Configuration']
            row.update(version=state.get('Version'), code_sha256=state.get('CodeSha256'), state=state.get('State'), update_status=state.get('LastUpdateStatus'))
            with urllib.request.urlopen(deployed['Code']['Location'], timeout=30) as response:
                archive = zipfile.ZipFile(io.BytesIO(response.read()))
            source_dir = root / 'aws/lambdas' / function / 'source'
            tracked = subprocess.check_output(['git', 'ls-files', '-z', str(source_dir.relative_to(root))], cwd=root).decode().split('\0')
            files = [root / p for p in tracked if p]
            members = [(p.relative_to(source_dir).as_posix(), p) for p in files]
            members += [(p.name, p) for p in shared_imports(root, files) if not (source_dir / p.name).exists()]
            for name, path in members:
                expected = hashlib.sha256(path.read_bytes()).hexdigest()
                actual = hashlib.sha256(archive.read(name)).hexdigest() if name in archive.namelist() else None
                row['files'].append({'member': name, 'expected_sha256': expected, 'actual_sha256': actual, 'match': expected == actual})
            row['configuration_mismatches'] = [remote for local, remote in (('runtime', 'Runtime'), ('handler', 'Handler'), ('timeout', 'Timeout'), ('memory', 'MemorySize'), ('architectures', 'Architectures')) if local in config and config[local] != state.get(remote)]
            row['pass'] = bool(row['files']) and all(x['match'] for x in row['files']) and not row['configuration_mismatches'] and state.get('State') == 'Active' and state.get('LastUpdateStatus') == 'Successful'
        except Exception as exc:
            # Exceptions can embed signed download URLs: record type only.
            row['error_type'] = type(exc).__name__
        result.append(row)
    return result
