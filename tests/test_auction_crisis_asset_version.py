"""Actual source page and stamper; no page execution or data access."""
from pathlib import Path
import hashlib,json,sys,tempfile
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
import stamp_assets

def main():
    contract=json.loads((ROOT/'tests/fixtures/auction-crisis-asset/preservation.json').read_bytes())
    previous=(ROOT/contract['page']['predecessor']).read_bytes();current=(ROOT/'auction-crisis.html').read_bytes()
    assert hashlib.sha256(previous).hexdigest()==contract['page']['previous_sha256']
    assert previous.count(contract['old'].encode())==1
    assert current==previous.replace(contract['old'].encode(),contract['new'].encode())
    asset=(ROOT/'auction-crisis.js').read_bytes()
    with tempfile.TemporaryDirectory(prefix='auction-asset-') as temporary:
        site=Path(temporary);page=site/'auction-crisis.html';script=site/'auction-crisis.js'
        page.write_bytes(previous);script.write_bytes(asset)
        versions,mode=stamp_assets.compute_versions(site);assert mode=='topo';stamp_assets.rewrite_html(site,versions)
        assert contract['old'].encode() in page.read_bytes(),'Predecessor no longer reproduces unstamped URL'
        page.write_bytes(current);seen=set()
        for suffix in (b'',b'\n/* invented offline asset revision */\n'):
            script.write_bytes(asset+suffix);versions,mode=stamp_assets.compute_versions(site);assert mode=='topo'
            version=versions['/auction-crisis.js'];assert version==hashlib.md5(script.read_bytes()).hexdigest()[:8];assert version not in seen;seen.add(version)
            stamp_assets.rewrite_html(site,versions);assert page.read_bytes().count(('src="/auction-crisis.js?v='+version+'"').encode())==1
            before={p.name:p.read_bytes() for p in site.iterdir()};again,repeat_mode=stamp_assets.compute_versions(site);stamp_assets.rewrite_html(site,again)
            assert again==versions and repeat_mode==mode;assert before=={p.name:p.read_bytes() for p in site.iterdir()}
    print('Auction Crisis stale import reproduced; two content versions and idempotence verified')

if __name__=='__main__':main()
