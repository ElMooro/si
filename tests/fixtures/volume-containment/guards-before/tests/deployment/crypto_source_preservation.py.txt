from pathlib import Path
import hashlib,json
ROOT=Path(__file__).resolve().parents[2]
D=ROOT/'tests/fixtures/worker-crypto-source'
def restore_crypto_source(raw):
    t=json.loads((D/'transition.json').read_bytes())
    digest=lambda s:hashlib.sha256(s.encode()).hexdigest()
    assert digest(raw)==t['candidate_sha256']
    assert raw.count(t['after'])==1
    raw=raw.replace(t['after'],t['before'])
    assert digest(raw)==t['source_sha256']
    assert raw==(D/'predecessor/index.js').read_text(encoding='utf-8')
    return raw
