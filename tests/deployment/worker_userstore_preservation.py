from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parents[2]
# Same peel as tests/helpers/worker-userstore-preservation.cjs peelHeatRange.
# Stored worker is the raw blob; fixture after_sha256 is the peeled blob.
_HEAT_AFTER = "4c4c7c9bb4ebec4bea6a733c722bda35991e8ea1ac3500d9a35d1dcbd41b4c99"
_HEAT_BEFORE = "128f5848c65c334aaddc03b668de0f0a025a7b14fb69e6a00ac555a42dae1015"
_HEAT_FROM = '        const yUrl = range === "max"\n'
_HEAT_TO = '        const yUrl = range === "max" || forceYahoo\n'

def peel_heat_range(raw, file):
    if file != "cloudflare/workers/justhodl-data-proxy/src/index.js":
        return raw
    if hashlib.sha256(raw.encode()).hexdigest() != _HEAT_AFTER:
        return raw
    assert raw.count(_HEAT_FROM) == 1, "yf range"
    raw = raw.replace(_HEAT_FROM, _HEAT_TO, 1)
    assert hashlib.sha256(raw.encode()).hexdigest() == _HEAT_BEFORE, file
    return raw

def peel_edge_revalidate(raw, file):
    """ops 6551 (2026-10-20): revalidated edge copies; same peel as peelEdgeRevalidate in the .cjs helper.  Without it
    every Lambda deploy failed this gate from commit 12246d7f on (the JS chain was taught, this one was not)."""
    t = json.loads((R/'tests/fixtures/worker-edge-revalidate/transition.json').read_bytes())
    if file != t['path'] or hashlib.sha256(raw.encode()).hexdigest() != t['after_sha256']:
        return raw
    for e in reversed(t['replacements']):
        assert raw.count(e['after']) == 1, "edge revalidate hunk"
        raw = raw.replace(e['after'], e['before'], 1)
    assert hashlib.sha256(raw.encode()).hexdigest() == t['before_sha256'], file
    return raw

def peel_crawler_gate(raw, file):
    """ops 6628 (2026-10-23): crawler gate; same peel as the .cjs helper's crawler-gate block.  The JS chain was taught
    at 14261948e, this one was not, so every Lambda deploy failed preflight until this peel (same trap as ops 6551)."""
    t = json.loads((R/'tests/fixtures/worker-crawler-gate/transition.json').read_bytes())
    if file != t['path'] or hashlib.sha256(raw.encode()).hexdigest() != t['after_sha256']:
        return raw
    for e in reversed(t['replacements']):
        assert raw.count(e['after']) == 1, "crawler gate hunk"
        raw = raw.replace(e['after'], e['before'], 1)
    assert hashlib.sha256(raw.encode()).hexdigest() == t['before_sha256'], file
    return raw

def normalize(raw,file):
    raw = peel_crawler_gate(raw, file)
    raw = peel_edge_revalidate(raw, file)
    raw = peel_heat_range(raw, file)
    t=json.loads((R/'tests/fixtures/worker-userstore/transition.json').read_bytes())['entries'].get(file)
    if not t:
        return raw
    digest=lambda s:hashlib.sha256(s.encode()).hexdigest()
    assert digest(raw)==t['after_sha256']
    for e in reversed(t['replacements']):
        assert raw.count(e['after'])==1
        raw=raw.replace(e['after'],e['before'])
    assert digest(raw)==t['before_sha256']
    assert raw==(R/t['before_path']).read_text(encoding='utf-8')
    return raw
