"""Reverse only exact reviewed deltas before existing complete-source assertions."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
TRANSITION=json.loads((ROOT/'tests/fixtures/chart-oecd/transition.json').read_bytes())
def normalize(file,raw):
    row=TRANSITION['changes'].get(file)
    if not row:return raw
    if hashlib.sha256(raw).hexdigest()==row['before_sha256']:return raw
    assert hashlib.sha256(raw).hexdigest()==row['after_sha256'],file
    text=raw.decode('utf-8').encode('utf-16-le')
    for edit in reversed(row['edits']):
        start,end=edit['start']*2,edit['end']*2
        assert text[start:end].decode('utf-16-le')==edit['after'],file
        text=text[:start]+edit['before'].encode('utf-16-le')+text[end:]
    before=text.decode('utf-16-le').encode('utf-8')
    assert hashlib.sha256(before).hexdigest()==row['before_sha256'],file
    assert before==(ROOT/row['before_path']).read_bytes(),file
    return before
