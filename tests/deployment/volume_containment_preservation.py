"""Exact inert source inverse for the reviewed Worker; no runtime data repair."""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parents[2]
D = ROOT / 'tests/fixtures/volume-containment'

def restore_volume_containment(raw):
    t = json.loads((D / 'source-transitions.json').read_text(encoding='utf-8'))['sources']['worker']
    digest = lambda s: hashlib.sha256(s.encode('utf-8')).hexdigest()
    assert digest(raw) == t['after_sha256'], 'exact reviewed containment candidate'
    for e in reversed(t['edits']):
        assert raw.count(e['after']) == 1, 'unique reviewed site'
        raw = raw.replace(e['after'], e['before'], 1)
    old = (ROOT / t['before_path']).read_text(encoding='utf-8')
    assert digest(old) == t['before_sha256']
    assert raw == old and digest(raw) == t['before_sha256']
    return raw
