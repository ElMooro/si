"""Reverse only the exact batch optimization before historical release gates."""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parents[1]


def preceding_source(path, text):
    relative = Path(path).relative_to(ROOT).as_posix()
    plans = json.loads((ROOT / 'tests/fixtures/ticker-batch/transition.json').read_text(encoding='utf-8'))
    if relative not in plans:
        return text
    plan = plans[relative]
    assert hashlib.sha256(text.encode()).hexdigest() == plan['after_sha256'], relative
    for before, after in reversed(plan['edits']):
        assert text.count(after) == 1, relative
        text = text.replace(after, before)
    assert hashlib.sha256(text.encode()).hexdigest() == plan['before_sha256'], relative
    return text
