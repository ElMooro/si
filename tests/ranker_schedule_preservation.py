"""Reverse the exact schedule documentation edit before historical ranker gates."""
from pathlib import Path
import hashlib
import json
ROOT = Path(__file__).resolve().parents[1]


def preceding_source(path, text):
    path = Path(path)
    if not path.is_absolute():
        path = ROOT / path
    plan = json.loads((ROOT/'tests/fixtures/master-ranker-schedule/transition.json').read_text(encoding='utf-8'))
    if path.relative_to(ROOT).as_posix() != plan['path']:
        return text
    assert hashlib.sha256(text.encode()).hexdigest() == plan['after_sha256']
    assert text.count(plan['new']) == 1
    text = text.replace(plan['new'], plan['old'])
    assert hashlib.sha256(text.encode()).hexdigest() == plan['before_sha256']
    return text
