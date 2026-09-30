"""Complete title parsing and last-good publication; no external requests."""
import importlib.util
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
from unittest.mock import patch

R = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('navigation_publication', R / 'scripts/gen_nav_manifest.py')
nav = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nav)
OLD = R / 'tests/fixtures/navigation/pre-unicode-generator.py.txt'


def refuses(fn, errors=(ValueError, UnicodeError, OSError, TypeError)):
    try:
        fn()
    except errors:
        return
    raise AssertionError('Invalid navigation publication accepted')


def test_complete_late_title_and_script_content():
    with tempfile.TemporaryDirectory() as directory:
        p = Path(directory) / 'page.html'
        p.write_text('<html><head><script>const x="<title>invented fake</title>";</script>' + ' ' * 5000 +
                     '<TITLE lang="en">Caf&eacute; &amp; Rates &#x26A1; | JustHodl.AI</TITLE></head></html>', encoding='utf-8')
        assert nav.title_of(p) == 'Café & Rates ⚡'
        old = runpy.run_path(str(OLD))
        assert old['title_of'](p) == 'invented fake'


def test_entities_decoded_exactly_once_and_title_rcdata_preserved():
    with tempfile.TemporaryDirectory() as directory:
        p = Path(directory) / 'page.html'
        p.write_text('<title>Literal &amp;lt;tag&amp;gt; and <b>text</b> Ω</title>', encoding='utf-8')
        assert nav.title_of(p) == 'Literal &lt;tag&gt; and <b>text</b> Ω'


def test_whitespace_and_decoded_brand_suffix():
    with tempfile.TemporaryDirectory() as directory:
        p = Path(directory) / 'page.html'
        p.write_text('<title> A\n B &middot; JustHodl.AI </title>', encoding='utf-8')
        assert nav.title_of(p) == 'A B'


def test_missing_empty_and_redirect_title_behavior():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for name, value in [('missing', '<h1>Untitled</h1>'), ('empty', '<title> </title>'), ('redirect', '<title>Redirecting</title>')]:
            (root / (name + '.html')).write_text(value, encoding='utf-8')
        (root / 'kept.html').write_text('<title>Kept</title>', encoding='utf-8')
        with patch.object(nav, 'ROOT', root), patch.object(nav, 'CUR', root / 'nav-manifest.json'):
            nav.main()
        result = json.loads((root / 'nav-manifest.json').read_bytes())
        assert result['n_pages'] == 1 and result['categories'][0]['pages'] == [{'href': '/kept.html', 'title': 'Kept'}]


def test_incomplete_duplicate_invalid_encoding_titles_refused():
    with tempfile.TemporaryDirectory() as directory:
        p = Path(directory) / 'page.html'
        for raw in (b'<title>open', b'<title>one</title><title>two</title>', b'<title>bad\xff</title>'):
            p.write_bytes(raw)
            refuses(lambda: nav.title_of(p))
        refuses(lambda: nav.title_of(p.parent / 'missing.html'))


def test_failed_replace_preserves_entire_previous_manifest():
    with tempfile.TemporaryDirectory() as directory:
        p = Path(directory) / 'nav-manifest.json'; prior = b'{"last_good":"entire previous manifest"}\n'; p.write_bytes(prior)
        with patch.object(nav.os, 'replace', side_effect=OSError('invented replace failure')):
            refuses(lambda: nav.write_manifest(p, {'title': 'Ω'}))
        assert p.read_bytes() == prior and list(p.parent.glob('nav-manifest.json.*.tmp')) == []


def test_serialization_failure_preserves_entire_previous_manifest():
    with tempfile.TemporaryDirectory() as directory:
        p = Path(directory) / 'nav-manifest.json'; prior = b'{"last_good":"whole"}'; p.write_bytes(prior)
        refuses(lambda: nav.write_manifest(p, {'bad': object()}))
        assert p.read_bytes() == prior and list(p.parent.glob('nav-manifest.json.*.tmp')) == []


def test_invalid_source_does_not_replace_manifest():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); p = root / 'nav-manifest.json'; prior = b'{"categories":[]}'; p.write_bytes(prior)
        (root / 'good.html').write_text('<title>Good</title>', encoding='utf-8')
        (root / 'bad.html').write_bytes(b'<title>bad\xff</title>')
        with patch.object(nav, 'ROOT', root), patch.object(nav, 'CUR', p):
            refuses(nav.main)
        assert p.read_bytes() == prior


def test_manifest_runs_with_utf8_mode_disabled_and_preserves_category():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); dest = root / 'nav-manifest.json'
        (root / 'page.html').write_text('<title>Rates Ω &amp; FX</title>', encoding='utf-8')
        dest.write_text(json.dumps({'categories': [{'name': 'Café', 'pages': [{'href': '/page.html'}]}]}, ensure_ascii=False), encoding='utf-8')
        script = ('import importlib.util; from pathlib import Path; s=importlib.util.spec_from_file_location("n",' +
                  repr(str(R / 'scripts/gen_nav_manifest.py')) + '); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); m.ROOT=Path(' +
                  repr(str(root)) + '); m.CUR=m.ROOT/"nav-manifest.json"; m.main()')
        result = subprocess.run([sys.executable, '-X', 'utf8=0', '-c', script], capture_output=True,
            env={**os.environ, 'PYTHONUTF8': '0', 'PYTHONCOERCECLOCALE': '0', 'LC_ALL': 'C'})
        assert result.returncode == 0, result.stderr
        value = json.loads(dest.read_bytes())
        assert value['title_encoding'] == 'unicode_text'
        assert value['categories'] == [{'name': 'Café', 'count': 1, 'pages': [{'href': '/page.html', 'title': 'Rates Ω & FX'}]}]


def test_atomic_success_replaces_whole_document_without_temporary_leftovers():
    with tempfile.TemporaryDirectory() as directory:
        p = Path(directory) / 'nav-manifest.json'; p.write_bytes(b'{"previous":"whole"}')
        data = {'categories': [{'name': 'Ω', 'pages': [{'title': 'Café & FX'}]}]}
        nav.write_manifest(p, data)
        assert p.read_bytes() == json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert list(p.parent.glob('nav-manifest.json.*.tmp')) == []
