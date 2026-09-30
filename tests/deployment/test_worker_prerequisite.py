"""A native private publisher cannot precede its exact retained Worker protocol."""
from pathlib import Path
import copy
import hashlib
import io
import json
import runpy
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
SCOPE = runpy.run_path(str(ROOT/'scripts/check_worker_prerequisite.py'))


def refuses(fn):
    try: fn()
    except SCOPE['PrerequisiteError']: pass
    else: raise AssertionError('Unproven prerequisite accepted')


def fixture(root):
    capture = b'{"complete_invented_code_capture":true}\n'
    path = 'aws/ops/reports/worker-source/release-123.json'
    (root/path).parent.mkdir(parents=True); (root/path).write_bytes(capture)
    expected = {'commit': 'a'*40, 'repository_files': {'synthetic.js': {'bytes': 1, 'sha256': 'b'*64}}}
    receipt = {**copy.deepcopy(expected), 'contract': 'worker-release.v1', 'status': 'matched', 'worker': SCOPE['WORKER'],
        'source_active_version_binding_verified': True, 'intended_repo_build_verified': True,
        'capture_artifact': {'path': path, 'sha256': hashlib.sha256(capture).hexdigest()}}
    (root/SCOPE['KEY']).parent.mkdir(parents=True); (root/SCOPE['KEY']).write_text(json.dumps(receipt), encoding='utf-8')
    return receipt, expected


def test_ordered_worker_prerequisite_requires_complete_current_source_and_retained_capture():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); receipt, expected = fixture(root)
        result = SCOPE['validate'](receipt, expected, root)
        assert result['commit'] == 'a'*40 and result['worker_invocations'] == result['private_reads'] == 0
        for key, value in [('commit', 'c'*40), ('status', 'queued'), ('worker', 'other'), ('source_active_version_binding_verified', 1),
                           ('intended_repo_build_verified', False), ('repository_files', {})]:
            bad = copy.deepcopy(receipt); bad[key] = value
            refuses(lambda: SCOPE['validate'](bad, expected, root))
        for size in [True, 1.0]:
            bad = copy.deepcopy(receipt); bad['repository_files']['synthetic.js']['bytes'] = size
            refuses(lambda: SCOPE['validate'](bad, expected, root))
        (root/receipt['capture_artifact']['path']).write_bytes(b'truncated')
        refuses(lambda: SCOPE['validate'](receipt, expected, root))


def test_ordered_worker_prerequisite_uses_only_exact_public_receipt_and_closes_body():
    seen = []
    class Client:
        def __init__(self, raw, metadata=None): self.body = io.BytesIO(raw); self.metadata = metadata or {}
        def get_object(self, **request): seen.append(request); return {'Body': self.body, **self.metadata}
    client = Client(b'{"complete":true}', {'ContentLength': 17})
    assert SCOPE['read_receipt'](client, time.monotonic()+20) == {'complete': True}; assert client.body.closed
    assert seen == [{'Bucket': 'justhodl-dashboard-live', 'Key': 'data/ops/releases/worker-justhodl-data-proxy.json'}]
    for raw, metadata in [(b'{}', {'ContentLength': True}), (b'{}', {'ContentLength': 3}), (b'{}', {'ContentEncoding': 'gzip'}),
                          (b'{"a":1,"a":2}', {}), (b'\xff', {}), (b' '*2_000_001, {}),
                          (b'{"x":1e999}', {}), (b'{"x":"\\ud800"}', {})]:
        client = Client(raw, metadata); refuses(lambda: SCOPE['read_receipt'](client, time.monotonic()+20)); assert client.body.closed


def test_ordered_worker_source_identity_refuses_uncommitted_build_inputs():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); source = root/SCOPE['WORKER_PATH']/'src'; source.mkdir(parents=True)
        (source/'portfolio-publication.js').write_text('export const synthetic = true;\n', encoding='utf-8')
        def git(*args): return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL)
        git('init'); git('add', '.'); git('-c', 'user.name=Synthetic', '-c', 'user.email=synthetic@example.invalid', 'commit', '-m', 'Synthetic')
        identity = SCOPE['source_identity'](root)
        assert identity['commit'] == git('rev-parse', 'HEAD').decode().strip()
        (source/'unexpected.js').write_text('export const unexpected = true;\n', encoding='utf-8')
        refuses(lambda: SCOPE['source_identity'](root))


def test_ordered_worker_check_occurs_before_native_code_or_schedule_mutation():
    shell = (ROOT/'scripts/deploy_lambdas.sh').read_text(encoding='utf-8')
    guard = shell.index('scripts/check_worker_prerequisite.py')
    assert shell.index('if [ "$fn" = "justhodl-portfolio-risk" ]') < guard < shell.index('aws lambda update-function-code')
    assert guard < shell.index('scripts/protect_lambda_alias.py')
    source = (ROOT/'scripts/check_worker_prerequisite.py').read_text(encoding='utf-8')
    assert '.invoke(' not in source and '.put_object(' not in source
    assert 'scripts/check_worker_prerequisite.py' in (ROOT/'.github/workflows/deploy-lambdas.yml').read_text(encoding='utf-8')


if __name__ == '__main__':
    tests = [v for k, v in list(globals().items()) if k.startswith('test_') and callable(v)]
    for test in tests: test()
    print(f'Worker prerequisite tests: {len(tests)} passed')
