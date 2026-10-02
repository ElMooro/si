from pathlib import Path
import hashlib,json
D=Path(__file__).resolve().parents[1]/'fixtures/ticker-input-reader'
def normalize_shared(text):
 t=json.loads((D/'transition.json').read_bytes())
 assert hashlib.sha256(text.encode()).hexdigest()==t['after_sha256']
 for e in reversed(t['replacements']):
  assert text.count(e['after'])==1;text=text.replace(e['after'],e['before'])
 assert text==(D/'shared-before.py.txt').read_text(encoding='utf-8')
 assert hashlib.sha256(text.encode()).hexdigest()==t['before_sha256']
 return text
