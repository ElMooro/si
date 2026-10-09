"""Reverse the exact risk-composite release edits (ops 6518) before older preservation gates.

Only the website-research-status import manifest changed: classic-dashboard.html re-pointed its two Risk Sizer links to
/risk-sizer.html when /risk.html became the composite risk page, and the manifest records those two replacements plus the
new page hash.  Both complete-source hashes are checked; unexpected edits fail.
"""
from pathlib import Path
import hashlib
import json
ROOT = Path(__file__).resolve().parents[1]


def preceding_source(path, text):
    path = Path(path)
    if not path.is_absolute():
        path = ROOT / path
    plans = json.loads((ROOT/'tests/fixtures/risk-composite/transition.json').read_text(encoding='utf-8'))['sources']
    relative = path.relative_to(ROOT).as_posix()
    if relative not in plans:
        return text
    plan = plans[relative]
    assert hashlib.sha256(text.encode()).hexdigest() == plan['after_sha256'], relative
    for old, new in reversed(plan['edits']):
        assert text.count(new) == 1, (relative, 'unexpected risk-composite edit')
        text = text.replace(new, old)
    assert hashlib.sha256(text.encode()).hexdigest() == plan['before_sha256'], relative
    return text
