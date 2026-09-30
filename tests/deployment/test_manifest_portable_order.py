"""Whole source inventories must not depend on host Path comparison rules."""
import hashlib
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from gen_engine_manifest import build


def engine(root, name):
    source = root / 'aws/lambdas' / name / 'source'
    source.mkdir(parents=True)
    (source.parent / 'config.json').write_text(json.dumps({'handler': 'lambda_function.lambda_handler'}), encoding='utf-8')
    (source / 'lambda_function.py').write_text("def lambda_handler(event,context):\n client.put_object(Key='data/direct.json')\n", encoding='utf-8')
    return source


def test_engine_array_is_ordered_by_exact_name_on_every_host():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for name in ('Zulu', 'aardvark', 'Beta'):
            engine(root, name)
        actual = build(root)
        assert [row['engine'] for row in actual['engines']] == ['Beta', 'Zulu', 'aardvark']
        assert all(row['keys'] == ['data/direct.json'] for row in actual['engines'])


def test_nested_candidate_evidence_has_portable_order_and_posix_paths():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); source = engine(root, 'invented')
        nested = source / 'helpers'; nested.mkdir()
        for name in ('aardvark.py', 'Zulu.py'):
            (nested / name).write_text("def unused():\n client.put_object(Key='data/candidate.json')\n", encoding='utf-8')
        (nested / 'unused.js').write_text('export const dormant = true;\n', encoding='utf-8')
        row = build(root)['engines'][0]
        assert [proof['file'] for proof in row['write_evidence']['data/candidate.json']] == ['helpers/Zulu.py', 'helpers/aardvark.py']
        assert all(proof['entrypoint_reachability'] == 'unproven' for proof in row['write_evidence']['data/candidate.json'])
        assert row['output_reachability']['data/candidate.json']['status'] == 'unproven'
        assert any(item.get('file') == 'helpers/unused.js' for item in row['unresolved_writes'])


def test_complete_predecessor_and_current_inventory_are_retained():
    raw = (ROOT / 'tests/fixtures/pre-source-contract-compilation-gen_engine_manifest.py.txt').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '5d052a66e303454172e76c2cf02cf72b99828b74807643795cad68ed3ea628f5'
    manifest = json.loads((ROOT / 'engine-manifest.json').read_bytes())
    names = [row['engine'] for row in manifest['engines']]
    assert names == sorted(names) and len(names) == len(set(names)) == manifest['n_engines']
    # Preserve the whole observed Linux predecessor without freezing future
    # engine changes to it. Release acceptance separately compares all records.
    raw_capture = (ROOT / 'tests/fixtures/pre-portable-order-live-engine-manifest.json').read_bytes()
    assert hashlib.sha256(raw_capture).hexdigest() == '5230cddb5eb40d600c216c3709d5ee5d273ce327214bbeffa0d2d666bf62b779'
    captured = json.loads(raw_capture)
    captured_names = [row['engine'] for row in captured['engines']]
    assert captured_names == sorted(captured_names)
    assert len(captured_names) == len(set(captured_names)) == captured['n_engines'] == 893
