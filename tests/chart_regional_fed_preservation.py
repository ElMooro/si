from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parents[1]
def normalize(file,raw):
    from chart_regional_surveys_preservation import normalize as newer
    raw=newer(file,raw)
    manifest=json.loads((R/'tests/fixtures/chart-regional-fed/transition.json').read_bytes());t=manifest['changes'].get(file)
    if not t:return raw
    digest=lambda s:hashlib.sha256(s).hexdigest()
    if digest(raw)==t['before_sha256']:return raw
    if digest(raw)!=t['after_sha256']:
        assert digest(raw) in manifest['predecessor_hashes'].get(file,[]),file
        return raw
    text=raw.decode('utf-8')
    for e in reversed(t['replacements']):
        assert text.count(e['after'])==1,file
        text=text.replace(e['after'],e['before'])
    before=text.encode('utf-8');assert digest(before)==t['before_sha256'],file;assert before==(R/t['before_path']).read_bytes(),file
    return before
