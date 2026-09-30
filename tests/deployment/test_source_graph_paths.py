"""Reuse only the canonical names already bound to this graph's loaded modules."""
import hashlib
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from gen_engine_manifest import Scan, imported_symbols
from source_write_graph import SharedWriteGraph


def fixture(root, key='data/invented-first.json'):
    source = root / 'aws/lambdas/invented/source'
    shared = root / 'aws/shared'
    source.mkdir(parents=True, exist_ok=True); shared.mkdir(parents=True, exist_ok=True)
    entry = source / 'lambda_function.py'
    entry.write_text('from writer import publish\ndef lambda_handler(event,context):\n publish()\n', encoding='utf-8')
    writer = shared / 'writer.py'
    writer.write_text(f'raise RuntimeError("source must remain inert")\ndef publish():\n client.put_object(Key={key!r})\n', encoding='utf-8')
    return source, entry, writer


def test_loaded_module_names_do_not_reenter_filesystem_resolution():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); source, entry, writer = fixture(root)
        (source / 'nested').mkdir()
        graph = SharedWriteGraph(root, source, {}, Scan, imported_symbols)
        out = graph.analyze(source / 'nested/../lambda_function.py', 'lambda_handler')
        assert out['keys'] == {'data/invented-first.json'}
        expected = {entry.resolve(): 'aws/lambdas/invented/source/lambda_function.py', writer.resolve(): 'aws/shared/writer.py'}
        with patch.object(Path, 'resolve', side_effect=AssertionError('loaded source path resolved twice')):
            for _ in range(100):
                for path, label in expected.items():
                    assert graph.relative(path) == label
        assert out['proofs']['data/invented-first.json'][0]['repository_path'] == 'aws/shared/writer.py'


def test_unknown_paths_keep_resolution_and_repository_containment_checks():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory) / 'repo'; source, entry, _ = fixture(root)
        graph = SharedWriteGraph(root, source, {}, Scan, imported_symbols)
        (source / 'nested').mkdir()
        assert graph.relative(source / 'nested/../lambda_function.py') == 'aws/lambdas/invented/source/lambda_function.py'
        outside = Path(directory) / 'outside.py'; outside.write_text('pass\n', encoding='utf-8')
        try:
            graph.relative(outside)
        except ValueError:
            pass
        else:
            raise AssertionError('Outside source was given repository ownership')


def test_new_graph_reads_new_source_and_does_not_share_previous_bindings():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); source, entry, _ = fixture(root)
        before = SharedWriteGraph(root, source, {}, Scan, imported_symbols).analyze(entry, 'lambda_handler')
        fixture(root, 'data/invented-second.json')
        after = SharedWriteGraph(root, source, {}, Scan, imported_symbols).analyze(entry, 'lambda_handler')
        assert before['keys'] == {'data/invented-first.json'}
        assert after['keys'] == {'data/invented-second.json'}


def test_complete_predecessor_compilers_are_retained_without_execution():
    expected = {'source_write_graph.py': '0f025b0037fceede6daef7f8fec3a5724d202a725b10b12f001b8873c46403d2',
                'build_page_data_contracts.py': '79bfe3d14acde2773bb3cff05117838afd8ef747d22b5683e72f56f74b556798'}
    for name, digest in expected.items():
        raw = (ROOT / 'tests/fixtures' / ('pre-source-contract-compilation-' + name + '.txt')).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == digest
