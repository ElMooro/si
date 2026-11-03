"""Reverse only the trigger-cap edits (chain_guard v2, 2026-11-02) before the earlier whole-source assertions.

The fleet-wide trigger cap put three lines into justhodl-symdir: the chain_guard import, the guarded Lambda client in
place of the raw one, and chain_guard.begin(event, context) at handler entry.  Nothing else in the module changed; this
layer takes exactly those lines out so every older preservation layer sees the source it was written against."""
from pathlib import Path
import hashlib, json
R = Path(__file__).resolve().parents[1]


def normalize(file, raw):
    manifest = json.loads((R / 'tests/fixtures/trigger-cap/transition.json').read_bytes()); t = manifest['changes'].get(file)
    if not t: return raw
    digest = lambda s: hashlib.sha256(s).hexdigest()
    if digest(raw) == t['before_sha256']: return raw
    if digest(raw) != t['after_sha256']:
        assert digest(raw) in manifest['predecessor_hashes'].get(file, []), file
        return raw
    text = raw.decode('utf-8')
    for e in reversed(t['replacements']):
        assert text.count(e['after']) == 1, file
        text = text.replace(e['after'], e['before'])
    before = text.encode('utf-8'); assert digest(before) == t['before_sha256'], file; assert before == (R / t['before_path']).read_bytes(), file
    return before
