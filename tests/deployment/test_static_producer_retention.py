"""Retain static source proof across the real ops retention selection/sweep."""
from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import sys
import tarfile
import tempfile
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from build_page_data_contracts import contract
from gen_engine_manifest import ast_keys

SOURCE = 'scripts/static-producers/ops_4281_alpha_atlas.py.txt'
OLD_SOURCE = 'aws/ops/ran/ops_4281_alpha_atlas.py'
SOURCE_SHA256 = '522046df7f369156caa8344b51a446df29e72c91623e655d64e029bc4b87a6b3'
ROUTE = 'alpha-atlas.html'
KEY = 'data/alpha-atlas.json'


def write(root, path, body):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)


def fixture(root, source=SOURCE):
    role = json.loads((ROOT / 'config/page-role-overrides.json').read_text(encoding='utf-8'))['pages'][ROUTE]
    role['static_outputs'][0]['source'] = source
    write(root, 'config/page-role-overrides.json', json.dumps({'pages': {ROUTE: role}}).encode('utf-8'))
    write(root, 'engine-manifest.json', b'{"schema_version":"engine-manifest.v2","engines":[]}')
    write(root, ROUTE, ('<!doctype html><script>fetch("/' + KEY + '");</script>').encode('utf-8'))
    write(root, source, (ROOT / SOURCE).read_bytes())


def sweep(root):
    # Run the production selection, archive and removal commands verbatim in a
    # disposable repository; stop before its commit/pull/push publication tail.
    workflow = (ROOT / '.github/workflows/ops-retention.yml').read_text(encoding='utf-8')
    body = workflow.split('        run: |\n', 1)[1].split('          git config user.email', 1)[0]
    body = textwrap.dedent(body).replace("${{ github.event.inputs.days || '60' }}", '60')
    # Keep the workflow's intermediate lists private to this test repository.
    temp = root / 'retention-lists'
    temp.mkdir()
    # Git Bash accepts drive-qualified forward slashes; quote the directory so
    # a Windows backslash or a workspace space cannot alter the shell program.
    body = body.replace('/tmp/', shlex.quote(temp.resolve().as_posix()) + '/')
    subprocess.run(['bash', '-c', body], cwd=root, check=True, capture_output=True, text=True)


def retained_repo(root, source=SOURCE):
    fixture(root, source)
    write(root, OLD_SOURCE, (ROOT / SOURCE).read_bytes())
    write(root, 'aws/ops/reports/old.json', b'{"synthetic":true}')
    write(root, 'aws/ops/reports/_lastrun.log', b'synthetic retained run log\n')
    env = dict(os.environ, GIT_CONFIG_GLOBAL='/dev/null', GIT_CONFIG_NOSYSTEM='1',
               GIT_AUTHOR_NAME='Retention Test', GIT_AUTHOR_EMAIL='test@example.invalid',
               GIT_COMMITTER_NAME='Retention Test', GIT_COMMITTER_EMAIL='test@example.invalid')
    def git(*args, date=None):
        dated = dict(env)
        if date:
            dated.update(GIT_AUTHOR_DATE=date, GIT_COMMITTER_DATE=date)
        return subprocess.check_output(['git', *args], cwd=root, env=dated, text=True).strip()
    git('init', '-q')
    git('add', '.')
    git('commit', '-qm', 'old synthetic ops and retained source', date='2000-01-01T00:00:00+00:00')
    write(root, 'aws/ops/ran/ops_9999_recent.py', b'# synthetic recent operation\n')
    git('add', '.')
    recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    git('commit', '-qm', 'recent synthetic operation', date=recent)
    return git


def test_static_producer_is_exact_historical_source_with_consistent_references():
    raw = (ROOT / SOURCE).read_bytes()
    assert len(raw) == 9618 and hashlib.sha256(raw).hexdigest() == SOURCE_SHA256
    written, _, parsed = ast_keys(raw.decode('utf-8'))
    assert parsed and KEY in written
    role = json.loads((ROOT / 'config/page-role-overrides.json').read_text(encoding='utf-8'))['pages'][ROUTE]
    assert role['static_outputs'] == [{'engine': 'ops:alpha-atlas', 'key': KEY, 'source': SOURCE}]
    assert any(row.get('file') == SOURCE and row.get('line') == 199 for row in role['evidence'])
    assert not any(row.get('file') == OLD_SOURCE for row in role['evidence'])
    checked = json.loads((ROOT / 'config/page-data-contracts.json').read_text(encoding='utf-8'))['pages'][ROUTE]
    output = next(row for row in checked['outputs'] if row['engine'] == 'ops:alpha-atlas')
    assert output['key'] == KEY and output['ownership_evidence'] == [{'file': SOURCE, 'basis': 'bound write argument'}]
    assert not (ROOT / OLD_SOURCE).exists()


def test_real_retention_sweep_preserves_static_contract_and_archives_eligible_ops():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        git = retained_repo(root)
        before = contract(root)
        sweep(root)
        assert not (root / OLD_SOURCE).exists()
        assert not (root / 'aws/ops/reports/old.json').exists()
        assert (root / 'aws/ops/ran/ops_9999_recent.py').is_file()
        assert (root / 'aws/ops/reports/_lastrun.log').is_file()
        assert hashlib.sha256((root / SOURCE).read_bytes()).hexdigest() == SOURCE_SHA256
        assert SOURCE in git('ls-files').splitlines()
        archives = list((root / 'aws/ops/_archive').glob('*.tar.gz'))
        assert len(archives) == 1
        with tarfile.open(archives[0]) as archive:
            assert set(archive.getnames()) == {OLD_SOURCE, 'aws/ops/reports/old.json'}
            assert archive.extractfile(OLD_SOURCE).read() == (ROOT / SOURCE).read_bytes()
        after = contract(root)
        assert after == before
        page = after['pages'][ROUTE]
        assert page['primary_output_status'] == 'ACCESSIBLE_BY_CONTRACT'
        assert page['coverage_class'] == 'PRIMARY_VALID_CONTRACT'
        assert page['outputs'] == [{'engine': 'ops:alpha-atlas', 'key': KEY, 'access': 'public',
                                  'inspection_schema': 'json-value.v1',
                                  'ownership_evidence': [{'file': SOURCE, 'basis': 'bound write argument'}]}]


GATE_SOURCES = ('ops_5233_retry_audit_core_release.py', 'ops_5235_retry_alias_protected_release.py', 'ops_5236_retry_pinned_core_release.py',
                'ops_5237_retry_pinned_core_release.py', 'ops_5239_retry_pinned_core_release.py', 'ops_5240_page_api_identity.py')


def test_real_retention_sweep_keeps_what_a_test_names():
    """2026-11-08: the weekly sweep archived six ops sources that test_release_recovery / test_page_api_identity load by
    path, and this runner - the preflight of every engine deploy - could no longer import. An artifact a test names
    stays loose, however old; everything else that is old is still archived."""
    for name in GATE_SOURCES:
        assert (ROOT / 'aws/ops/ran' / name).is_file() or (ROOT / 'aws/ops/pending' / name).is_file(), name
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        named, report, engine_named = 'aws/ops/ran/ops_5240_page_api_identity.py', 'aws/ops/reports/latest/ops_6084_share structure part 2.md', 'aws/ops/ran/ops_5271_audit_page_api_probe.py'
        named_report = 'aws/ops/reports/latest/ops_6370_snapshot_storage_configuration.md'
        write(root, named, b'# synthetic gate source\n')
        write(root, engine_named, b'# synthetic gate source named by an engine test\n')
        write(root, named_report, b'# synthetic report a test reads\n')
        write(root, report, b'# synthetic old report with spaces in its name\n')
        write(root, 'tests/deployment/test_page_api_identity.py', b"OPERATION = ROOT / 'aws/ops/pending/ops_5240_page_api_identity.py'\nREPORT = 'ops_6370_snapshot_storage_configuration.md'\n")
        write(root, 'aws/lambdas/justhodl-x/tests/run_tests.py', b"SRC = 'aws/ops/ran/ops_5271_audit_page_api_probe.py'\n")
        write(root, 'docs/notes.md', b'ops_4281_alpha_atlas.py is named here, outside any test\n')
        retained_repo(root)
        sweep(root)
        for kept in (named, engine_named, named_report, 'aws/ops/ran/ops_9999_recent.py', 'aws/ops/reports/_lastrun.log'):
            assert (root / kept).is_file(), kept
        for swept in (OLD_SOURCE, report, 'aws/ops/reports/old.json'):
            assert not (root / swept).exists(), swept
        archives = list((root / 'aws/ops/_archive').glob('*.tar.gz'))
        assert len(archives) == 1
        with tarfile.open(archives[0]) as archive:
            assert set(archive.getnames()) == {OLD_SOURCE, report, 'aws/ops/reports/old.json'}


def test_former_ops_path_contract_reproduces_the_failure_after_retention():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        retained_repo(root, OLD_SOURCE)
        assert contract(root)['pages'][ROUTE]['coverage_class'] == 'PRIMARY_VALID_CONTRACT'
        sweep(root)
        try:
            contract(root)
        except FileNotFoundError as exc:
            assert Path(exc.filename) == root / OLD_SOURCE
        else:
            raise AssertionError('missing static source must stop compilation')


def test_retained_static_proof_still_requires_present_source():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        fixture(root)
        (root / SOURCE).unlink()
        try:
            contract(root)
        except FileNotFoundError as exc:
            assert Path(exc.filename) == root / SOURCE
        else:
            raise AssertionError('missing retained proof passed')


def test_retained_static_proof_still_rejects_malformed_and_wrong_output_source():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        fixture(root)
        for raw in (b'def broken(:\n', b's3.put_object(Key="data/unrelated.json")\n', b'# no writes\n'):
            write(root, SOURCE, raw)
            try:
                contract(root)
            except ValueError as exc:
                assert str(exc) == 'Static output ownership or access invalid: ' + ROUTE + ' ' + KEY
            else:
                raise AssertionError('invalid static source proof passed')


def test_retained_static_proof_still_rejects_private_output():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        fixture(root)
        private = 'data/brain.json'
        path = root / 'config/page-role-overrides.json'
        doc = json.loads(path.read_text(encoding='utf-8'))
        doc['pages'][ROUTE]['static_outputs'][0]['key'] = private
        path.write_text(json.dumps(doc), encoding='utf-8')
        write(root, SOURCE, ('s3.put_object(Key="' + private + '")\n').encode('utf-8'))
        try:
            contract(root)
        except ValueError as exc:
            assert str(exc) == 'Static output ownership or access invalid: ' + ROUTE + ' ' + private
        else:
            raise AssertionError('private static output passed')


if __name__ == '__main__':
    tests = [value for name, value in sorted(globals().items()) if name.startswith('test_') and callable(value)]
    for test in tests:
        test()
    print('Static producer retention tests passed: ' + str(len(tests)))
