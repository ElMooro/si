"""A platform code page must not change source identity or published page text."""
from pathlib import Path
from unittest.mock import patch
import ast
import hashlib
import json
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import build_page_data_contracts as builder


def predecessor():
    raw = (ROOT / 'tests/fixtures/pre-contract-utf8-builder.py.txt').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '6fab70fe69a7058f1df0238b93a28666e59f71f5291a09e977794ae157e9baac'
    return ast.parse(raw.decode('utf-8'))


def codepage_defaults():
    read, write = Path.read_text, Path.write_text
    def read_text(path, *args, **kwargs):
        if not args and kwargs.get('encoding') is None:
            kwargs['encoding'] = 'cp1252'
        return read(path, *args, **kwargs)
    def write_text(path, data, *args, **kwargs):
        if not args and kwargs.get('encoding') is None:
            kwargs['encoding'] = 'cp1252'
        return write(path, data, *args, **kwargs)
    return patch.object(Path, 'read_text', read_text), patch.object(Path, 'write_text', write_text)


def test_non_ascii_source_identity_survives_windows_defaults():
    old = predecessor()
    assert any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='read_text' and not n.args and not n.keywords for n in ast.walk(old))
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        source = 'aws/lambdas/example/source/producer.py'
        code = 'def save():\n label = "München — €"\n s3.put_object(Key="data/cache/x.json")\n'
        (root / source).parent.mkdir(parents=True)
        (root / source).write_bytes(code.encode('utf-8'))
        digest = hashlib.sha256(ast.dump(ast.parse(code).body[0], include_attributes=False).encode()).hexdigest()
        role = {'engine': 'example', 'key': 'data/cache/*.json', 'role': 'internal_input_cache',
                'purpose': 'Retained provider observation input', 'public_access_approved': False,
                'evidence': {'source': source, 'functions': [{'name': 'save', 'ast_sha256': digest}]}}
        (root / 'config').mkdir()
        (root / 'config/engine-output-roles.json').write_bytes(json.dumps({'roles': [role]}, ensure_ascii=False).encode())
        engines = {'example': {'keys': [], 'key_patterns': ['data/cache/*.json'],
                   'write_evidence': {'data/cache/*.json': [{'repository_path': source, 'line': 3}]}}}
        reader, writer = codepage_defaults()
        with reader, writer:
            damaged=code.encode('utf-8').decode('cp1252')
            assert hashlib.sha256(ast.dump(ast.parse(damaged).body[0],include_attributes=False).encode()).hexdigest()!=digest
            assert builder.internal_output_roles(root, engines)['example']['data/cache/*.json'] == role
        assert (root / source).read_bytes() == code.encode('utf-8')


def test_html_installation_preserves_utf8_and_refuses_invalid_source_bytes():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td); site = root / 'site'; site.mkdir(); (root / 'config').mkdir()
        (root / 'jh-data-inspector.js').write_bytes(b'/* test asset */\n')
        (root / 'jh-evidence-io.js').write_bytes(b'/* test complete I/O asset */\n')
        page = site / 'test.html'
        source = '<html><head><title>München — 東京 Á</title></head><body>€ → USD\n</body></html>'
        doc = {'coverage': {}, 'pages': {'test.html': {'api_responses': [], 'repository_assets': []}}}
        reader, writer = codepage_defaults()
        with patch.object(builder, 'ROOT', root), patch.object(builder, 'contract', lambda _: doc), patch.object(sys, 'argv', ['builder', '--site', str(site)]), reader, writer:
            page.write_bytes(source.encode('utf-8'))
            builder.main()
            received = page.read_bytes()
            assert 'München — 東京 Á'.encode('utf-8') in received and '€ → USD'.encode('utf-8') in received
            assert b'\r\n' not in received and received.count(b'jh-data-inspector.js?v=') == 1
            builder.main()
            assert page.read_bytes() == received
            bad = b'<html><head></head><body>broken \xff</body></html>'
            page.write_bytes(bad)
            try:
                builder.main()
            except UnicodeDecodeError:
                pass
            else:
                raise AssertionError('Invalid UTF-8 must not be replaced and republished')
            assert page.read_bytes() == bad
