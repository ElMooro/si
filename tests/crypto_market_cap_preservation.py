"""Reverse only byte-exact reviewed Crypto edits before applying earlier gates."""
from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parents[1]
def preceding_source(path):
 path=Path(path);p=path.relative_to(R).as_posix();raw=path.read_bytes();plans=json.loads((R/'tests/fixtures/crypto-market-cap/edits.json').read_bytes())
 if p not in plans:
  # New sentiment consumers have a later complete predecessor, independently
  # checked before older unrelated-function assertions are applied.
  later=json.loads((R/'tests/fixtures/crypto-sentiment/edits.json').read_bytes())
  if p not in later:return raw.decode('utf-8')
  plan=later[p];assert hashlib.sha256(raw).hexdigest()==plan['candidate_sha256'],p
  text=raw.decode('utf-8')
  for old,new in reversed(plan['edits']):
   assert text.count(new)==1,(p,'unreviewed sentiment source change');text=text.replace(new,old)
  assert hashlib.sha256(text.encode()).hexdigest()==plan['predecessor_sha256'],p
  assert text.encode()==(R/'tests/fixtures/crypto-sentiment/before'/(p+'.txt')).read_bytes()
  return text
 plan=plans[p];assert hashlib.sha256(raw).hexdigest()==plan['candidate_sha256'],p
 text=raw.decode('utf-8')
 for old,new in reversed(plan['edits']):
  assert text.count(new)==1,(p,'unreviewed source change');text=text.replace(new,old)
 assert hashlib.sha256(text.encode()).hexdigest()==plan['predecessor_sha256'],p
 assert text.encode()==(R/'tests/fixtures/crypto-market-cap/before'/(p+'.txt')).read_bytes()
 return text
