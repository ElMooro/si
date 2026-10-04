"""Retain static source proof across the real ops retention selection/sweep."""
from __future__ import annotations

import hashlib
import json
import os
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
    body = body.replace('/tmp/', str(temp) + '/')
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
