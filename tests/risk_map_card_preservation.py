"""Reverse the exact Risk Map card edits (2026-11-24) before older preservation gates.

With engine 2.0.0 the Risk Map covers every asset class and its lists hold only what is in that state, so the home
dashboard's card names what is BOOMING and DESTROYED across the whole map, with the count, and says "none today" when
nothing is.  classic-dashboard.html changed in two places and the website-research-status import manifest records those
two replacements plus the new page hash.  Both complete-source hashes are checked; unexpected edits fail.
"""
from pathlib import Path
import hashlib
import json
ROOT = Path(__file__).resolve().parents[1]


def preceding_source(path, text):
    path = Path(path)
    if not path.is_absolute():
        path = ROOT / path
    plans = json.loads((ROOT/'tests/fixtures/risk-map-card/transition.json').read_text(encoding='utf-8'))['sources']
    relative = path.relative_to(ROOT).as_posix()
    if relative not in plans:
        return text
    plan = plans[relative]
    assert hashlib.sha256(text.encode()).hexdigest() == plan['after_sha256'], relative
    for old, new in reversed(plan['edits']):
        assert text.count(new) == 1, (relative, 'unexpected risk-map-card edit')
        text = text.replace(new, old)
    assert hashlib.sha256(text.encode()).hexdigest() == plan['before_sha256'], relative
    return text
