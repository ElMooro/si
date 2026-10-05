from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parents[1]
def normalize(file,raw):
    t=json.loads((R/'tests/fixtures/chart-imf/transition.json').read_bytes())['changes'].get(file)
    if not t:return raw
    digest=lambda s:hashlib.sha256(s).hexdigest()
    if digest(raw)==t['before_sha256']:return raw
    for name in ['chart-census-additional','chart-census-watchlist','chart-census','chart-oecd-additional','chart-oecd','chart-bis-fx','chart-market-repair','chart-bis','chart-cftc']:
        old=json.loads((R/('tests/fixtures/'+name+'/transition.json')).read_bytes())['changes'].get(file)
        if old and digest(raw) in [old['before_sha256'],old['after_sha256']]:return raw
    assert digest(raw)==t['after_sha256'],file
    text=raw.decode('utf-8')
    for e in reversed(t['replacements']):
        assert text.count(e['after'])==1,file
        text=text.replace(e['after'],e['before'])
    before=text.encode('utf-8');assert digest(before)==t['before_sha256'],file;assert before==(R/t['before_path']).read_bytes(),file
    return before
