#!/usr/bin/env python3
"""Receipt-only prerequisite before the native ordered portfolio publisher deploys.

No Worker invocation, private data access, source execution or mutation. Uses the
runner's existing S3 identity and exact checked-in Worker release evidence.
"""
from pathlib import Path
import argparse
import hashlib
import json
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
WORKER = 'justhodl-data-proxy'
WORKER_PATH = 'cloudflare/workers/' + WORKER
TOOLS = ['.github/workflows/deploy-workers.yml', 'scripts/worker_release.py', 'scripts/publish_worker_evidence.py',
         'aws/ops/checks/worker_source_evidence.py', 'aws/ops/checks/worker_release_evidence.py']
BUCKET = 'justhodl-dashboard-live'
KEY = 'data/ops/releases/worker-' + WORKER + '.json'
FUNCTIONS = ('justhodl-portfolio-risk', 'justhodl-portfolio-snapshot')
SNAPSHOT_ORIGIN = 'https://justhodl-data-proxy.raafouis.workers.dev'


class PrerequisiteError(RuntimeError):
    pass


def same(left, right):
    def encoded(value):
        return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
    try:
        return encoded(left) == encoded(right)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        return False


def source_identity(root=ROOT, function='justhodl-portfolio-risk'):
    if function not in FUNCTIONS:
        raise PrerequisiteError('Unreviewed native Worker consumer')
    paths = [WORKER_PATH, *TOOLS]
    if subprocess.run(['git', 'diff', '--exit-code', 'HEAD', '--', *paths], cwd=root, stdout=subprocess.DEVNULL).returncode:
        raise PrerequisiteError('Uncommitted Worker prerequisite input')
    extra = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z', '--', *paths], cwd=root)
    ignored = subprocess.check_output(['git', 'ls-files', '--others', '--ignored', '--exclude-standard', '-z', '--', WORKER_PATH+'/src'], cwd=root)
    if extra or ignored:
        raise PrerequisiteError('Uncommitted Worker prerequisite input')
    names = subprocess.check_output(['git', 'ls-files', '-z', '--', *paths], cwd=root).decode('utf-8').split('\0')
    required = [WORKER_PATH + '/src/portfolio-publication.js']
    if function == 'justhodl-portfolio-snapshot':
        required.append(WORKER_PATH + '/src/portfolio-snapshot.js')
    if not all(path in names for path in required):
        raise PrerequisiteError('Ordered Worker protocol source missing')
    files = {}
    for name in names:
        if not name:
            continue
        path = root/name
        if path.is_symlink():
            raise PrerequisiteError('External Worker prerequisite input')
        raw = path.read_bytes()
        files[name] = {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
    # A deployment can issue a new exact receipt without changing source bytes.
    # Pin the retained release, then prove its entire build tree matches HEAD;
    # the last source-edit commit is not necessarily the deployed commit.
    recorded = decode_receipt(committed_bytes(root, KEY))
    commit = recorded.get('commit') if type(recorded) is dict else None
    if type(commit) is not str or not re.fullmatch(r'[a-f0-9]{40}', commit):
        raise PrerequisiteError('Exact retained Worker release commit missing')
    if subprocess.run(['git', 'merge-base', '--is-ancestor', commit, 'HEAD'], cwd=root,
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        raise PrerequisiteError('Worker release is not in the checked-out history')
    historical = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', '-z', commit, '--', *paths], cwd=root)
    if set(historical.decode('utf-8').split('\0')) - {''} != set(files):
        raise PrerequisiteError('Worker release build inventory differs')
    for name, identity in files.items():
        raw = subprocess.check_output(['git', 'show', commit + ':' + name], cwd=root)
        if len(raw) != identity['bytes'] or hashlib.sha256(raw).hexdigest() != identity['sha256']:
            raise PrerequisiteError('Worker release build input differs from current source')
    expected = {'commit': commit, 'repository_files': files}
    validate(recorded, expected, root, function)
    committed_bytes(root, recorded['capture_artifact']['path'])
    return expected


def committed_bytes(root, name):
    path = root/name
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= 2_000_000:
        raise PrerequisiteError('Complete bounded retained Worker evidence required')
    raw = path.read_bytes()
    stored = subprocess.run(['git', 'show', 'HEAD:' + name], cwd=root, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    if stored.returncode or stored.stdout != raw:
        raise PrerequisiteError('Worker evidence differs from the checked-out commit')
    return raw


def read_receipt(client, deadline):
    response = client.get_object(Bucket=BUCKET, Key=KEY)
    body = response['Body']
    try:
        declared = response.get('ContentLength')
        if declared is not None and (type(declared) is not int or not 0 <= declared <= 2_000_000):
            raise PrerequisiteError('Invalid Worker receipt length')
        if response.get('ContentEncoding', 'identity') not in ('', 'identity'):
            raise PrerequisiteError('Encoded Worker receipt refused')
        parts, size = [], 0
        while True:
            if time.monotonic() >= deadline:
                raise PrerequisiteError('Worker receipt deadline')
            part = body.read(min(65536, 2_000_001-size))
            if time.monotonic() >= deadline:
                raise PrerequisiteError('Worker receipt deadline')
            if type(part) is not bytes:
                raise PrerequisiteError('Invalid Worker receipt bytes')
            if not part:
                break
            size += len(part)
            if size > 2_000_000:
                raise PrerequisiteError('Worker receipt exceeds bound')
            parts.append(part)
        if declared is not None and size != declared:
            raise PrerequisiteError('Incomplete Worker receipt')
    finally:
        body.close()
    return decode_receipt(b''.join(parts))


def decode_receipt(raw):
    def pairs(rows):
        out = {}
        for key, value in rows:
            if key in out:
                raise PrerequisiteError('Duplicate Worker receipt field')
            out[key] = value
        return out
    def constant(_): raise PrerequisiteError('Nonfinite Worker receipt value')
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=constant)
        # Escaped lone surrogates and exponent overflow must fail too.
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise PrerequisiteError('Invalid complete Worker receipt') from None


def validate(receipt, expected, root=ROOT, function='justhodl-portfolio-risk'):
    if function not in FUNCTIONS:
        raise PrerequisiteError('Unreviewed native Worker consumer')
    if (type(receipt) is not dict or receipt.get('contract') != 'worker-release.v1' or
            receipt.get('status') != 'matched' or receipt.get('worker') != WORKER or
            receipt.get('commit') != expected['commit'] or not same(receipt.get('repository_files'), expected['repository_files']) or
            receipt.get('source_active_version_binding_verified') is not True or receipt.get('intended_repo_build_verified') is not True):
        raise PrerequisiteError('Exact ordered Worker prerequisite is not deployed')
    recorded = json.loads((root/KEY).read_bytes())
    if not same(receipt, recorded):
        raise PrerequisiteError('Worker prerequisite differs from retained release evidence')
    capture = receipt.get('capture_artifact')
    if type(capture) is not dict or type(capture.get('path')) is not str or not re.fullmatch(r'aws/ops/reports/worker-source/release-[0-9]+\.json', capture['path']):
        raise PrerequisiteError('Complete Worker source capture missing')
    raw = (root/capture['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != capture.get('sha256'):
        raise PrerequisiteError('Retained Worker source capture differs')
    return {'function': function, 'worker': WORKER, 'commit': expected['commit'],
            'status': 'exact_prerequisite_matched', 'worker_invocations': 0, 'private_reads': 0}


def check_snapshot_origin(client):
    """Inspect only the existing non-secret origin setting; never log env data."""
    config = client.get_function_configuration(FunctionName='justhodl-portfolio-snapshot')
    environment = config.get('Environment', {})
    if type(environment) is not dict or environment.get('Error'):
        raise PrerequisiteError('Snapshot publication origin configuration unavailable')
    variables = environment.get('Variables', {})
    if type(variables) is not dict:
        raise PrerequisiteError('Snapshot publication origin configuration unavailable')
    origin = variables.get('PRIVATE_ARTIFACT_PROXY', SNAPSHOT_ORIGIN)
    if type(origin) is not str or origin.rstrip('/') != SNAPSHOT_ORIGIN:
        raise PrerequisiteError('Snapshot publication origin differs from reviewed adapter')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--wait', type=int, default=180)
    parser.add_argument('--function', choices=FUNCTIONS, default='justhodl-portfolio-risk')
    args = parser.parse_args()
    if not 0 <= args.wait <= 300:
        raise PrerequisiteError('Bounded prerequisite wait required')
    import boto3
    from botocore.config import Config
    expected = source_identity(function=args.function)
    client = boto3.client('s3', region_name=args.region, config=Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts': 2}))
    end = time.monotonic() + args.wait
    while True:
        try:
            receipt = read_receipt(client, time.monotonic()+20)
            result = validate(receipt, expected, function=args.function)
            if args.function == 'justhodl-portfolio-snapshot':
                native = boto3.client('lambda', region_name=args.region, config=Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts': 2}))
                result['reviewed_snapshot_origin_verified'] = check_snapshot_origin(native)
            print(json.dumps(result))
            return
        except Exception:
            if time.monotonic() >= end:
                raise PrerequisiteError('Ordered Worker prerequisite not verified; native deployment refused') from None
            print('Waiting for exact retained Worker prerequisite receipt', flush=True)
            time.sleep(min(10, max(0, end-time.monotonic())))


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Ordered Worker prerequisite not verified; native deployment refused', file=sys.stderr)
        sys.exit(1)
