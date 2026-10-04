from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parents[2]
def normalize(raw,file):
    t=json.loads((R/'tests/fixtures/worker-oecd-path/transition.json').read_bytes())['entries'][file]
    digest=lambda s:hashlib.sha256(s.encode()).hexdigest()
    assert digest(raw)==t['after_sha256']
    for e in reversed(t['replacements']):
        assert raw.count(e['after'])==1
        raw=raw.replace(e['after'],e['before'])
    assert digest(raw)==t['before_sha256']
    assert raw==(R/t['before_path']).read_text(encoding='utf-8')
    return raw
