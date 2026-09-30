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
TOOLS = ['.github/workflows/deploy-workers.yml', 'scripts/worker_release.py',
         'aws/ops/checks/worker_source_evidence.py', 'aws/ops/checks/worker_release_evidence.py']
BUCKET = 'justhodl-dashboard-live'
KEY = 'data/ops/releases/worker-' + WORKER + '.json'


class PrerequisiteError(RuntimeError):
    pass


def same(left, right):
    def encoded(value):
        return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
    try:
        return encoded(left) == encoded(right)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        return False


def source_identity(root=ROOT):
    paths = [WORKER_PATH, *TOOLS]
    subprocess.run(['git', 'diff', '--exit-code', 'HEAD', '--', *paths], cwd=root, stdout=subprocess.DEVNULL, check=True)
    extra = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z', '--', *paths], cwd=root)
    ignored = subprocess.check_output(['git', 'ls-files', '--others', '--ignored', '--exclude-standard', '-z', '--', WORKER_PATH+'/src'], cwd=root)
    if extra or ignored:
        raise PrerequisiteError('Uncommitted Worker prerequisite input')
    names = subprocess.check_output(['git', 'ls-files', '-z', '--', *paths], cwd=root).decode('utf-8').split('\0')
    required = WORKER_PATH + '/src/portfolio-publication.js'
    if required not in names:
        raise PrerequisiteError('Ordered Worker protocol source missing')
    commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', *paths], cwd=root, text=True).strip()
    if not re.fullmatch(r'[a-f0-9]{40}', commit):
        raise PrerequisiteError('Exact Worker prerequisite commit missing')
    files = {}
    for name in names:
        if not name:
            continue
        path = root/name
        if path.is_symlink():
            raise PrerequisiteError('External Worker prerequisite input')
        raw = path.read_bytes()
        files[name] = {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
    return {'commit': commit, 'repository_files': files}


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
    def pairs(rows):
        out = {}
        for key, value in rows:
            if key in out:
                raise PrerequisiteError('Duplicate Worker receipt field')
            out[key] = value
        return out
    def constant(_): raise PrerequisiteError('Nonfinite Worker receipt value')
    try:
        value = json.loads(b''.join(parts).decode('utf-8'), object_pairs_hook=pairs, parse_constant=constant)
        # Escaped lone surrogates and exponent overflow must fail too.
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise PrerequisiteError('Invalid complete Worker receipt') from None


def validate(receipt, expected, root=ROOT):
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
    return {'function': 'justhodl-portfolio-risk', 'worker': WORKER, 'commit': expected['commit'],
            'status': 'exact_prerequisite_matched', 'worker_invocations': 0, 'private_reads': 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--wait', type=int, default=180)
    args = parser.parse_args()
    if not 0 <= args.wait <= 300:
        raise PrerequisiteError('Bounded prerequisite wait required')
    import boto3
    from botocore.config import Config
    expected = source_identity()
    client = boto3.client('s3', region_name=args.region, config=Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts': 2}))
    end = time.monotonic() + args.wait
    while True:
        try:
            receipt = read_receipt(client, time.monotonic()+20)
            print(json.dumps(validate(receipt, expected)))
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
