"""Reverse only the exact recorded network integration before older release gates.

Both complete-source hashes remain checked. Unexpected edits fail; historical
fixtures and their hashes are never regenerated or weakened.
"""
from pathlib import Path
import hashlib
import json
ROOT = Path(__file__).resolve().parents[1]


def preceding_source(path, text=None):
    path = Path(path)
    if not path.is_absolute(): path = ROOT / path
    relative = path.relative_to(ROOT).as_posix()
    text = path.read_text(encoding='utf-8') if text is None else text
    from ticker_batch_preservation import preceding_source as before_batch
    text = before_batch(path, text)
    plans = json.loads((ROOT/'tests/fixtures/research-network/transition.json').read_text(encoding='utf-8'))['sources']
    if relative not in plans: return text
    plan = plans[relative]
    assert hashlib.sha256(text.encode()).hexdigest() == plan['after_sha256'], relative
    for old, new in reversed(plan['edits']):
        assert text.count(new) == 1, (relative, 'unexpected integration edit')
        text = text.replace(new, old)
    assert hashlib.sha256(text.encode()).hexdigest() == plan['before_sha256'], relative
    return text
