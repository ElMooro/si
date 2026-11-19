"""Reverse only the 2026-11-19 policy-pin block in justhodl-symdir before the earlier whole-source assertions.

Khalid: "Fix all of them" (typing sp500 led with FRED's copy; the index never appeared).  The directory's search now
lists its policy pins whether or not the query words matched the pinned document.  That is one block inside search();
this layer takes exactly that block out so every older preservation layer sees the source it was written against."""
from pathlib import Path
import hashlib, json
R = Path(__file__).resolve().parents[1]


def normalize(file, raw):
    manifest = json.loads((R / 'tests/fixtures/chart-index-routes/python-transition.json').read_bytes()); t = manifest['changes'].get(file)
    if not t: return raw
    digest = lambda s: hashlib.sha256(s).hexdigest()
    if digest(raw) != t['after_sha256']: return raw
    text = raw.decode('utf-8')
    for e in reversed(json.loads((R / t['replacements_path']).read_bytes())):
        assert text.count(e['after']) == 1, file
        text = text.replace(e['after'], e['before'])
    before = text.encode('utf-8'); assert digest(before) == t['before_sha256'], file
    return before
