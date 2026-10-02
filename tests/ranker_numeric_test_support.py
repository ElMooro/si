"""Preserve the prior exact-source regression through the numeric transition."""
from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parents[1]
D=R/'tests/fixtures/ranker-numeric'
def prior_source(source):
    transition=json.loads((D/'transition.json').read_bytes())
    assert hashlib.sha256(source.encode()).hexdigest()==transition['after_sha256']
    for change in reversed(transition['changes']):
        assert source.count(change['after'])==1
        source=source.replace(change['after'],change['before'])
    assert hashlib.sha256(source.encode()).hexdigest()==transition['before_sha256']
    assert source==(D/'lambda-before.py.txt').read_text(encoding='utf-8')
    return source
