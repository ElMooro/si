"""Real Git publication against complete invented histories and local bare remotes.

Only current code executes. The entire predecessor workflow is inert evidence.
No AWS, GitHub, Cloudflare or private data access is used by these tests.
"""
from pathlib import Path
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/'scripts'), str(ROOT/'aws/ops/checks'), str(Path(__file__).parent)]
import publish_worker_evidence as pub
import worker_release as cli
from worker_source_evidence import EvidenceError
from test_worker_release_evidence import evidence, CODE


def git(root, *args, check=True):
    result = subprocess.run(['git', *args], cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check:
        assert result.returncode == 0, (args, result.stderr.decode(errors='replace'))
    return result


def write(root, name, raw):
    path = root/name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw if type(raw) is bytes else raw.encode())


def commit(root, message):
    git(root, 'add', '.')
    git(root, '-c', 'user.name=Invented', '-c', 'user.email=invented@example.invalid', 'commit', '-m', message)
    return git(root, 'rev-parse', 'HEAD').stdout.decode().strip()


def refuses(function, match=None):
    try:
        function()
    except Exception as exc:
        assert isinstance(exc, EvidenceError), type(exc)
        if match:
            assert match in str(exc), str(exc)
    else:
        raise AssertionError('Unsafe evidence publication was accepted')


class History:
    def __init__(self, root):
        self.root = Path(root)
        self.remote = self.root/'remote.git'
        git(self.root, 'init', '--bare', '--initial-branch=main', str(self.remote))
        self.author = self.clone('author')
        for name in pub.TOOLS:
            write(self.author, name, '# Complete invented build input\n')
        write(self.author, cli.WORKER_PATH+'/src/index.js', CODE)
        write(self.author, cli.WORKER_PATH+'/wrangler.toml', 'name="justhodl-data-proxy"\n')
        self.original = evidence()
        self.original['captured_at'] = '2026-09-30T12:00:00+00:00'
        write(self.author, pub.BASELINE, json.dumps(self.original))
        self.source = commit(self.author, 'Complete invented source tree and original control-plane fixture')
        git(self.author, 'push', 'origin', 'HEAD:main')
        self.build = self.root/'build'
        write(self.build, 'index.js', CODE)

    def clone(self, name):
        root = self.root/name
        git(self.root, '-c', 'core.autocrlf=false', 'clone', '--quiet', str(self.remote), str(root))
        git(root, 'config', 'core.autocrlf', 'false')
        return root

    def release(self, root, run_id, minute):
        before = copy.deepcopy(self.original)
        after = copy.deepcopy(self.original)
        before['captured_at'] = f'2026-09-30T12:{minute:02d}:00+00:00'
        after['captured_at'] = f'2026-09-30T12:{minute:02d}:30+00:00'
        after['version_id'] = '00000000-0000-4000-8000-'+str(run_id).zfill(12)
        after['deployment_id'] = '10000000-0000-4000-8000-'+str(run_id).zfill(12)
        folder = self.root/('capture-'+str(run_id))
        with patch.object(cli, 'ROOT', root), patch.object(cli, 'BASELINE', root/pub.BASELINE), patch.object(cli, 'safe_capture', side_effect=[before, after]):
            for phase in ('before', 'after'):
                cli.execute(phase, self.build, folder, self.source, str(run_id))
        return str(run_id)

    def publish(self, root, run_id, **kwargs):
        return pub.publish(root, self.build, self.source, run_id, **kwargs)

    def remote_bytes(self, name):
        return git(self.remote, 'show', 'main:'+name).stdout

    def add_remote(self, name, raw):
        git(self.author, 'pull', '--ff-only', 'origin', 'main')
        write(self.author, name, raw)
        value = commit(self.author, 'Invented concurrent author change')
        git(self.author, 'push', 'origin', 'HEAD:main')
        return value


def test_worker_evidence_two_old_checkouts_retain_both_captures_and_latest_receipt():
    with tempfile.TemporaryDirectory() as folder:
        h = History(folder)
        first, second = h.clone('first'), h.clone('second')
        one, two = h.release(first, 100, 1), h.release(second, 101, 2)
        before = git(second, 'rev-parse', 'HEAD').stdout
        h.publish(first, one)
        h.add_remote('docs/invented-unrelated.md', 'Retain this whole unrelated edit.\n')
        result = h.publish(second, two)
        assert result['status'] == 'retained' and result['attempts'] == 1
        assert git(second, 'rev-parse', 'HEAD').stdout == before
        for root, run_id in ((first, one), (second, two)):
            name = 'aws/ops/reports/worker-source/release-'+run_id+'.json'
            assert h.remote_bytes(name) == (root/name).read_bytes()
        assert h.remote_bytes(pub.RECEIPT) == (second/pub.RECEIPT).read_bytes()
        assert h.remote_bytes('docs/invented-unrelated.md') == b'Retain this whole unrelated edit.\n'


def test_worker_evidence_reproduces_mutable_receipt_rebase_conflict_in_invented_history():
    # Model the old algorithm with explicit Git operations; never execute an
    # archived workflow/source. Both complete receipt bodies come from current code.
    with tempfile.TemporaryDirectory() as folder:
        h = History(folder)
        initial = h.clone('initial');run = h.release(initial, 99, 1);h.publish(initial, run)
        h.source = git(h.remote, 'rev-parse', 'main').stdout.decode().strip()
        first, second = h.clone('first'), h.clone('second')
        for root, run_id, minute in ((first, 100, 2), (second, 101, 3)):
            h.release(root, run_id, minute)
            commit(root, 'Invented competing mutable receipt')
        git(first, 'push', 'origin', 'HEAD:main')
        git(second, 'fetch', 'origin', 'main')
        result = git(second, '-c', 'user.name=Invented', '-c', 'user.email=invented@example.invalid', 'rebase', 'origin/main', check=False)
        assert result.returncode != 0
        assert pub.RECEIPT.encode() in git(second, 'diff', '--name-only', '--diff-filter=U').stdout
        git(second, 'rebase', '--abort')


def test_worker_evidence_retries_a_real_fast_forward_race_without_text_merging():
    with tempfile.TemporaryDirectory() as folder:
        h = History(folder);root = h.clone('runner');run = h.release(root, 100, 1)
        original = pub.git;count = []
        def racing(where, *args, **kwargs):
            if args[0] == 'push' and not count:
                count.append(True);h.add_remote('docs/race.txt', 'Whole concurrent document\n')
            return original(where, *args, **kwargs)
        with patch.object(pub, 'git', side_effect=racing):
            result = h.publish(root, run)
        assert result['attempts'] == 2
        assert h.remote_bytes('docs/race.txt') == b'Whole concurrent document\n'
        assert h.remote_bytes(pub.RECEIPT) == (root/pub.RECEIPT).read_bytes()


def test_worker_evidence_idempotent_retry_and_lost_push_acknowledgement():
    with tempfile.TemporaryDirectory() as folder:
        h = History(folder);root = h.clone('runner');run = h.release(root, 100, 1)
        original = pub.git
        def lost(where, *args, **kwargs):
            result = original(where, *args, **kwargs)
            if args[0] == 'push':
                result.returncode = 1
            return result
        with patch.object(pub, 'git', side_effect=lost):
            result = h.publish(root, run)
        assert result['status'] == 'retained'
        current = git(h.remote, 'rev-parse', 'main').stdout
        assert h.publish(root, run)['idempotent'] is True
        assert git(h.remote, 'rev-parse', 'main').stdout == current


def test_worker_evidence_preserves_callers_index_worktree_and_head():
    with tempfile.TemporaryDirectory() as folder:
        h = History(folder);root = h.clone('runner');run = h.release(root, 100, 1)
        write(root, 'notes.txt', 'Staged caller content\n');git(root, 'add', 'notes.txt')
        write(root, 'notes.txt', 'Unstaged caller content\n')
        index = (root/'.git/index').read_bytes()
        head = git(root, 'rev-parse', 'HEAD').stdout
        status = git(root, 'status', '--porcelain=v1', '-uall').stdout
        h.publish(root, run)
        assert (root/'.git/index').read_bytes() == index
        assert git(root, 'rev-parse', 'HEAD').stdout == head
        assert git(root, 'status', '--porcelain=v1', '-uall').stdout == status
        assert (root/'notes.txt').read_bytes() == b'Unstaged caller content\n'


def test_worker_evidence_refuses_immutable_capture_conflict():
    with tempfile.TemporaryDirectory() as folder:
        h = History(folder);root = h.clone('runner');run = h.release(root, 100, 1)
        name = 'aws/ops/reports/worker-source/release-100.json'
        h.add_remote(name, '{"whole_invented_conflicting_capture":true}\n')
        before = git(h.remote, 'rev-parse', 'main').stdout
        refuses(lambda:h.publish(root, run), 'Immutable')
        assert git(h.remote, 'rev-parse', 'main').stdout == before


def test_worker_evidence_refuses_late_older_or_simultaneous_release():
    for minute in (1, 2):
        with tempfile.TemporaryDirectory() as folder:
            h = History(folder);first, second = h.clone('first'), h.clone('second')
            one, two = h.release(first, 100, minute), h.release(second, 101, 2)
            h.publish(second, two);before = git(h.remote, 'rev-parse', 'main').stdout
            refuses(lambda:h.publish(first, one), 'newer or simultaneous')
            assert git(h.remote, 'rev-parse', 'main').stdout == before


def test_worker_evidence_refuses_source_drift_before_and_during_push():
    for during in (False, True):
        with tempfile.TemporaryDirectory() as folder:
            h = History(folder);root = h.clone('runner');run = h.release(root, 100, 1)
            original = pub.git;changed = []
            def drift():
                changed.append(True);h.add_remote(cli.WORKER_PATH+'/src/index.js', CODE+b'\n// New invented source\n')
            def racing(where, *args, **kwargs):
                if args[0] == 'push' and not changed:drift()
                return original(where, *args, **kwargs)
            if during:
                with patch.object(pub, 'git', side_effect=racing):
                    refuses(lambda:h.publish(root, run), 'build inputs changed')
            else:
                drift();refuses(lambda:h.publish(root, run), 'build inputs changed')
            assert not pub.tree_bytes(h.remote, 'main', pub.RECEIPT)


def test_worker_evidence_refuses_altered_or_truncated_local_evidence_before_fetch():
    for kind in ('capture', 'receipt', 'build', 'source', 'run'):
        with tempfile.TemporaryDirectory() as folder:
            h = History(folder);root = h.clone('runner');run = h.release(root, 100, 1)
            if kind == 'capture':write(root, 'aws/ops/reports/worker-source/release-100.json', '{"incomplete":')
            if kind == 'receipt':
                value = json.loads((root/pub.RECEIPT).read_bytes());value['commit'] = 'f'*40;write(root, pub.RECEIPT, json.dumps(value))
            if kind == 'build':write(h.build, 'index.js', CODE+b'\n// changed\n')
            if kind == 'source':write(root, cli.WORKER_PATH+'/src/index.js', CODE+b'\n// changed\n')
            if kind == 'run':run = '101'
            with patch.object(pub, 'fetch_main') as fetch:
                try:h.publish(root, run)
                except Exception:pass
                else:raise AssertionError('Damaged evidence accepted')
                fetch.assert_not_called()


def test_worker_evidence_bounded_retry_and_transport_failure_preserve_local_capture():
    with tempfile.TemporaryDirectory() as folder:
        h = History(folder);root = h.clone('runner');run = h.release(root, 100, 1)
        original = pub.git;attempts = []
        def moving(where, *args, **kwargs):
            if args[0] == 'push':
                attempts.append(True);h.add_remote('docs/moving.txt', 'Whole document revision '+str(len(attempts)))
            return original(where, *args, **kwargs)
        with patch.object(pub, 'git', side_effect=moving):
            refuses(lambda:h.publish(root, run, attempts=2), 'Main kept moving')
        assert len(attempts) == 2
        assert (root/'aws/ops/reports/worker-source/release-100.json').is_file()
        def failed(where, *args, **kwargs):
            if args[0] == 'push':return subprocess.CompletedProcess(args, 1, b'', b'Credentials must never be logged')
            return original(where, *args, **kwargs)
        with patch.object(pub, 'git', side_effect=failed):
            refuses(lambda:h.publish(root, run), 'not confirmed')


def test_worker_evidence_workflow_never_publishes_s3_before_git_retention():
    text = (ROOT/'.github/workflows/deploy-workers.yml').read_text(encoding='utf-8')
    block = text.split('      - name: Commit exact Worker receipt and full code capture\n',1)[1].split('      - name: Summary',1)[0]
    body = textwrap.dedent(block.split('        run: |\n',1)[1])
    assert 'git pull' not in body and 'git commit' not in body
    assert text.count('aws s3 cp data/ops/releases/worker-') == 1
    for failure in (0, 1):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder);write(root, 'aws/ops/reports/worker-source/release-100.json', '{}')
            # Bash functions replace only the boundary tools; run the complete
            # current workflow step. No real AWS/remote command can execute.
            preamble = ('function python3() { echo git-retention >> calls; return '+str(failure)+'; }\n'
                        'function aws() { echo public-receipt >> calls; }\n')
            write(root, 'run.sh', preamble+body)
            env = os.environ.copy();env.update(GITHUB_RUN_ID='100', GITHUB_SHA='a'*40, RUNNER_TEMP='invented')
            result = subprocess.run([shutil.which('bash'), 'run.sh'], cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assert (result.returncode == 0) == (failure == 0)
            expected = ['git-retention']+(['public-receipt'] if failure == 0 else [])
            assert (root/'calls').read_text(encoding='utf-8').splitlines() == expected


if __name__ == '__main__':
    tests = [v for k,v in globals().copy().items() if k.startswith('test_') and callable(v)]
    for test in tests:test()
    print('Worker evidence publisher tests:', len(tests), 'passed')
