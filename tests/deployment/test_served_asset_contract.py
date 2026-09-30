"""Complete built-file byte bindings, without mutable source-contract reuse."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import build_page_data_contracts as compiler


def fixture(root):
    doc={'schema':'invented-complete-contract','pages':{},'coverage':{'invented':True}}
    assets=[]
    for key in sorted(compiler.REPOSITORY_ASSETS):
        path=root/key;path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(json.dumps({'invented':key,'rows':[0,None,False,'source-only']},separators=(',',':')).encode())
        assets.append({'key':key,'role':'repository_configuration_or_asset','sha256':hashlib.sha256(b'pre-build representation').hexdigest()})
    doc['pages']={'index.html':{'repository_assets':assets,'outputs':[]},'second.html':{'repository_assets':deepcopy(assets),'outputs':[]}}
    return doc


def test_every_built_asset_hash_matches_exact_bytes_without_mutating_source_contract():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);doc=fixture(root);old=deepcopy(doc)
        bound=compiler.bind_site_assets(doc,root)
        assert doc==old and bound is not doc
        for page in bound['pages'].values():
            for asset in page['repository_assets']:
                assert asset['sha256']==hashlib.sha256((root/asset['key']).read_bytes()).hexdigest()
        changed=root/'config/home-layout.json';changed.write_bytes(b'{"invented":"updated"}')
        later=compiler.bind_site_assets(doc,root)
        assert later!=bound and doc==old


def test_missing_invalid_duplicate_and_nonfinite_built_assets_fail_before_mutation():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);doc=fixture(root);old=deepcopy(doc);target=root/'config/home-layout.json'
        for raw in (b'',b'not JSON',b'{"x":1,"x":2}',b'{"x":NaN}',b'\xff'):
            target.write_bytes(raw)
            try:compiler.bind_site_assets(doc,root)
            except (ValueError,UnicodeError):pass
            else:raise AssertionError('Invalid complete artifact received a hash')
            assert doc==old
        target.unlink()
        try:compiler.bind_site_assets(doc,root)
        except FileNotFoundError:pass
        else:raise AssertionError('Missing built asset fell back to source')


def test_unreviewed_or_escaping_asset_is_rejected_without_arbitrary_reads():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);doc=fixture(root)
        for key in ('../private.json','data/current.json','portfolio/snapshot.json'):
            bad=deepcopy(doc);bad['pages']['index.html']['repository_assets'][0]['key']=key
            with patch.object(Path,'read_bytes',side_effect=AssertionError('No unapproved read')):
                try:compiler.bind_site_assets(bad,root)
                except ValueError:pass
                else:raise AssertionError('Unreviewed path accepted')
        original=Path.resolve
        def escaped(path,*a,**kw):
            if path.name=='home-layout.json':return root.parent/'outside.json'
            return original(path,*a,**kw)
        with patch.object(Path,'resolve',escaped):
            try:compiler.bind_site_assets(doc,root)
            except ValueError as exc:assert 'escapes site root' in str(exc)
            else:raise AssertionError('Escaping artifact accepted')


def test_site_registry_and_embedded_page_receive_same_exact_binding():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td)/'repo';site=Path(td)/'site';root.mkdir();site.mkdir();(root/'config').mkdir()
        doc=fixture(site)
        for name in ('jh-data-inspector.js','jh-evidence-io.js'):(root/name).write_text('// whole invented helper',encoding='utf-8')
        page=site/'index.html';page.write_text('<html><head><meta charset="utf-8"></head><body>Invented</body></html>',encoding='utf-8')
        with patch.object(compiler,'ROOT',root),patch.object(compiler,'contract',return_value=doc),patch.object(compiler,'pages',return_value=[page]),patch.object(sys,'argv',['build','--site',str(site)]):compiler.main()
        source=json.loads((root/'config/page-data-contracts.json').read_bytes());served=json.loads((site/'config/page-data-contracts.json').read_bytes())
        assert source==doc and served==compiler.bind_site_assets(doc,site)
        import re
        embedded=json.loads(re.search(r'id="jh-page-data-contract"[^>]*>(.*?)</script>',page.read_text(encoding='utf-8')).group(1))
        assert embedded==served['pages']['index.html']


def test_whole_observed_predecessor_is_bound_to_original_build():
    raw=(ROOT/'tests/fixtures/pre-served-asset-live-contract.json').read_bytes()
    manifest=json.loads((ROOT/'tests/fixtures/pre-served-asset-live-build-manifest.json').read_bytes())
    assert manifest['commit_sha']=='c0087e1af923ac1899532ab7b47e12fd97a5ab2b'
    assert hashlib.sha256(raw).hexdigest()==manifest['files_sha256']['config/page-data-contracts.json']
    doc=json.loads(raw);rows={r['key']:r for r in doc['pages']['index.html']['repository_assets']}
    for key in ('config/home-layout.json','config/section-registry.json'):
        assert rows[key]['sha256']!=manifest['files_sha256'][key]
