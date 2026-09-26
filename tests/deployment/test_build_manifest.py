"""Complete deployment evidence includes nested pages and their own scripts."""
import hashlib
import runpy
import tempfile
from pathlib import Path

stamp = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/stamp_build_manifest.py"))["stamp"]


def test_nested_pages_and_scripts_have_independent_hashes_and_commit_stamps():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "nested").mkdir()
        (root / "index.html").write_text("<html><head></head></html>")
        (root / "nested/index.html").write_text("<html><HEAD></HEAD></html>")
        (root / "nested/app.js").write_text("const build = 1;")
        result = stamp(root, "a" * 40)
        assert result["page_count"] == 2
        assert result["files_sha256"]["nested/app.js"] == hashlib.sha256(b"const build = 1;").hexdigest()
        assert 'content="' + "a" * 40 + '"' in (root / "nested/index.html").read_text()
        before = result["files_sha256"]
        assert stamp(root, "a" * 40)["files_sha256"] == before


def test_registry_digest_is_exact_replaced_on_rebuild_and_removed_when_absent():
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);(root/'config').mkdir()
        page=root/'index.html';page.write_text('<html><head></head></html>')
        registry=root/'config/page-data-contracts.json';registry.write_bytes(b'{"first":true}\n')
        for body in (b'{"first":true}\n',b'{"second":false}\n'):
            registry.write_bytes(body);expected=hashlib.sha256(body).hexdigest()
            first=stamp(root,'b'*40)
            assert '<meta name="jh-data-registry-sha256" content="'+expected+'">' in page.read_text()
            assert first['files_sha256']['config/page-data-contracts.json']==expected
            assert page.read_text().count('jh-data-registry-sha256')==1
            assert stamp(root,'b'*40)['files_sha256']==first['files_sha256']
        registry.unlink();stamp(root,'c'*40)
        assert 'jh-data-registry-sha256' not in page.read_text()
