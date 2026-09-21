"""Old dated/semantic versions must not strand browsers on stale local code."""
from pathlib import Path
import hashlib, sys, tempfile
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'scripts'))
from stamp_assets import compute_versions, rewrite_html


def test_dated_and_semantic_versions_are_replaced_by_actual_content_hashes():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root/'app.js').write_text('const version = 2;')
        (root/'style.css').write_text('body { color: black; }')
        (root/'index.html').write_text('<script src="/app.js?v=20260921-native1"></script><link href="/style.css?v=release_1.2-beta">')
        versions, mode = compute_versions(root); rewrite_html(root, versions)
        source = (root/'index.html').read_text()
        assert mode == 'topo'
        assert '/app.js?v='+hashlib.md5((root/'app.js').read_bytes()).hexdigest()[:8] in source
        assert '/style.css?v='+versions['/style.css'] in source
        assert 'native1' not in source and 'release_' not in source


def test_changed_nested_dependency_rolls_referrer_and_page_and_is_idempotent():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root/'dep.js').write_text('const value = 1;')
        (root/'app.js').write_text('const src = "/dep.js?v=20260921-native1";')
        (root/'index.html').write_text('<script src="/app.js?v=v1.0-beta"></script>')
        before, _ = compute_versions(root); rewrite_html(root, before)
        (root/'dep.js').write_text('const value = 2;')
        after, _ = compute_versions(root); rewrite_html(root, after)
        assert after['/dep.js'] != before['/dep.js'] and after['/app.js'] != before['/app.js']
        assert '/dep.js?v='+after['/dep.js'] in (root/'app.js').read_text()
        assert '/app.js?v='+after['/app.js'] in (root/'index.html').read_text()
        stable, _ = compute_versions(root)
        assert stable == after and rewrite_html(root, stable)[0] == 0


def test_external_urls_other_parameters_and_service_worker_stay_unchanged():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root/'app.js').write_text('const version = 2;')
        (root/'service-worker.js').write_text('const version = 2;')
        original = '<script src="https://elsewhere.example/app.js?v=old-tag"></script><script src="/app.js?v=old-tag&mode=archive"></script><script src="/service-worker.js"></script>'
        (root/'index.html').write_text(original)
        versions, _ = compute_versions(root); rewrite_html(root, versions)
        assert '/service-worker.js' not in versions
        assert (root/'index.html').read_text() == original
