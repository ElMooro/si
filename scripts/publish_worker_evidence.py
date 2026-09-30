#!/usr/bin/env python3
"""Retain exact Worker evidence on a fresh main tree without rebasing receipts.

Uses an isolated temporary Git index and ordinary fast-forward pushes. Never
changes the caller's checkout/index/HEAD, invokes a Worker or publishes to S3.
The workflow publishes the public receipt only after this command succeeds.
"""
from datetime import datetime, timezone
from pathlib import Path
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'scripts'), str(ROOT/'aws/ops/checks')]
from worker_release import build_identity, WORKER_PATH, TOOLS
from worker_release_evidence import verify, WRANGLER_VERSION
from worker_source_evidence import EvidenceError, digest, encoded
from check_worker_prerequisite import decode_receipt

RECEIPT = 'data/ops/releases/worker-justhodl-data-proxy.json'
BASELINE = 'aws/ops/reports/worker-source/6354-36658515237.json'
LIMIT = 2_000_000
BOT = {'GIT_AUTHOR_NAME': 'github-actions[bot]',
       'GIT_AUTHOR_EMAIL': 'github-actions[bot]@users.noreply.github.com',
       'GIT_COMMITTER_NAME': 'github-actions[bot]',
       'GIT_COMMITTER_EMAIL': 'github-actions[bot]@users.noreply.github.com'}


def git(root, *args, data=None, env=None, check=True):
    result = subprocess.run(['git', *args], cwd=root, input=data, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
    if check and result.returncode:
        # Remote URLs and credential helper messages must not enter public logs.
        raise EvidenceError('Worker evidence Git operation failed: '+args[0])
    return result


def read_file(root, name):
    path = root/name
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise EvidenceError('Symlinked Worker evidence refused')
    if not path.is_file() or not 0 < path.stat().st_size <= LIMIT:
        raise EvidenceError('Complete bounded Worker evidence required')
    return path.read_bytes()


def tree_bytes(root, revision, name):
    result = git(root, 'ls-tree', '-z', revision, '--', name).stdout
    if not result:
        return None
    match = re.fullmatch(rb'100644 blob ([a-f0-9]{40})\t'+re.escape(name.encode())+rb'\x00', result)
    if not match:
        raise EvidenceError('Exact regular Worker evidence blob required')
    blob = match[1].decode()
    size = int(git(root, 'cat-file', '-s', blob).stdout)
    if not 0 < size <= LIMIT:
        raise EvidenceError('Stored Worker evidence exceeds bound')
    return git(root, 'cat-file', 'blob', blob).stdout


def instant(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)', value):
        raise EvidenceError('Exact UTC Worker capture time required')
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    except ValueError:
        raise EvidenceError('Invalid Worker capture time') from None


def validate_local(root, build_dir, commit, run_id):
    if type(run_id) is not str or not re.fullmatch(r'[0-9]+', run_id):
        raise EvidenceError('Exact runner identity required')
    identity = build_identity(build_dir, commit, root=root)
    receipt_raw = read_file(root, RECEIPT)
    receipt = decode_receipt(receipt_raw)
    path = 'aws/ops/reports/worker-source/release-'+run_id+'.json'
    raw = read_file(root, path)
    bundle = decode_receipt(raw)
    baseline = decode_receipt(read_file(root, BASELINE))
    if (type(receipt) is not dict or type(bundle) is not dict or
            bundle.get('contract') != 'worker-release-capture.v1' or
            receipt.get('capture_artifact') != {'path': path, 'sha256': digest(raw)} or
            receipt.get('workflow_run_id') != run_id or
            encoded(bundle.get('build')) != encoded(identity) or
            encoded(bundle.get('receipt')) != encoded({k:v for k,v in receipt.items() if k != 'capture_artifact'})):
        raise EvidenceError('Exact complete Worker evidence binding required')
    if encoded(bundle['before']['configuration']) != encoded(baseline['configuration']):
        raise EvidenceError('Original Worker configuration differs')
    checked = verify(bundle['after'], build_dir, commit, baseline['configuration'], WRANGLER_VERSION)
    if any(encoded(receipt.get(key)) != encoded(value) for key, value in checked.items()):
        raise EvidenceError('Worker receipt differs from verified complete build')
    if encoded(receipt.get('repository_files')) != encoded(identity['repository_files']):
        raise EvidenceError('Worker build source inventory differs')
    if instant(bundle['before']['captured_at']) > instant(receipt['captured_at']):
        raise EvidenceError('Worker capture chronology reversed')
    return path, raw, receipt_raw, receipt


def validate_base(root, base, commit, path, raw, receipt_raw, receipt):
    if git(root, 'merge-base', '--is-ancestor', commit, base, check=False).returncode:
        raise EvidenceError('Intended Worker source is outside current main history')
    if git(root, 'diff', '--exit-code', commit, base, '--', WORKER_PATH, *TOOLS, check=False).returncode:
        raise EvidenceError('Worker build inputs changed on main before evidence publication')
    existing = tree_bytes(root, base, path)
    if existing is not None and existing != raw:
        raise EvidenceError('Immutable Worker capture path already contains different bytes')
    old_raw = tree_bytes(root, base, RECEIPT)
    if old_raw == receipt_raw:
        if existing != raw:
            raise EvidenceError('Retained Worker receipt has no matching complete capture')
        return True
    if old_raw is not None:
        old = decode_receipt(old_raw)
        if (type(old) is not dict or old.get('contract') != 'worker-release.v1' or
                old.get('status') != 'matched' or old.get('worker') != receipt['worker'] or
                type(old.get('commit')) is not str or not re.fullmatch(r'[a-f0-9]{40}', old['commit'])):
            raise EvidenceError('Current Worker release identity is invalid')
        # A delayed earlier run must never move the release pointer backwards.
        # Equal capture times with different receipts are ambiguous and refused.
        if instant(old.get('captured_at')) >= instant(receipt['captured_at']):
            raise EvidenceError('A newer or simultaneous Worker receipt is already retained')
        if git(root, 'merge-base', '--is-ancestor', old['commit'], commit, check=False).returncode:
            raise EvidenceError('A newer Worker source receipt is already retained')
    return False


def fetch_main(root):
    git(root, 'fetch', '--quiet', 'origin', 'refs/heads/main')
    base = git(root, 'rev-parse', '--verify', 'FETCH_HEAD^{commit}').stdout.decode().strip()
    if not re.fullmatch(r'[a-f0-9]{40}', base):
        raise EvidenceError('Exact main commit unavailable')
    return base


def publish(root, build_dir, commit, run_id, attempts=5):
    if type(attempts) is not int or not 1 <= attempts <= 5:
        raise EvidenceError('Bounded publication attempts required')
    path, raw, receipt_raw, receipt = validate_local(root, build_dir, commit, run_id)
    # Pin validated bytes in memory. Never reread/rewrite mutable local evidence
    # while retrying a remote race; each candidate is exactly these two blobs.
    with tempfile.TemporaryDirectory(prefix='worker-evidence-index-') as folder:
        index = Path(folder)/'index'
        env = os.environ.copy()
        env.update(BOT, GIT_INDEX_FILE=str(index), GIT_TERMINAL_PROMPT='0')
        for attempt in range(1, attempts+1):
            base = fetch_main(root)
            already = validate_base(root, base, commit, path, raw, receipt_raw, receipt)
            if already:
                return {'status':'retained', 'commit':base, 'source_commit':commit,
                        'attempts':attempt, 'idempotent':True, 'capture':path}
            git(root, 'read-tree', base, env=env)
            for name, body in ((path, raw), (RECEIPT, receipt_raw)):
                blob = git(root, 'hash-object', '-w', '--stdin', data=body).stdout.decode().strip()
                git(root, 'update-index', '--add', '--cacheinfo', '100644,'+blob+','+name, env=env)
            tree = git(root, 'write-tree', env=env).stdout.decode().strip()
            message = 'Record exact Worker release '+commit[:10]+' [skip-deploy] [skip-ops]\n'
            candidate = git(root, 'commit-tree', tree, '-p', base, data=message.encode(), env=env).stdout.decode().strip()
            result = git(root, 'push', 'origin', candidate+':refs/heads/main', check=False, env=env)
            observed = fetch_main(root)
            # Also resolves a push whose acknowledgement was lost. Accept only
            # exact remotely retained bytes; a successful exit alone is no proof.
            if (git(root, 'merge-base', '--is-ancestor', candidate, observed, check=False).returncode == 0 and
                    tree_bytes(root, observed, path) == raw and tree_bytes(root, observed, RECEIPT) == receipt_raw):
                return {'status':'retained', 'commit':candidate, 'source_commit':commit,
                        'attempts':attempt, 'idempotent':False, 'capture':path}
            if result.returncode == 0 or observed == base:
                raise EvidenceError('Worker evidence push was not confirmed at remote main')
        raise EvidenceError('Main kept moving; Worker evidence retained only in Actions artifact')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', required=True)
    parser.add_argument('--commit', required=True)
    args = parser.parse_args()
    result = publish(ROOT, args.build_dir, args.commit, os.environ.get('GITHUB_RUN_ID', ''))
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('Worker evidence publication failed: '+type(exc).__name__+
              (': '+str(exc) if isinstance(exc, EvidenceError) else ''), file=sys.stderr)
        sys.exit(1)
