from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal
import base64,gzip,hashlib,json,unittest
import defillama_tvl as m
from test_defillama_tvl import Store
R=Path(__file__).resolve().parents[4]
class OriginalSourceTests(unittest.TestCase):
 def test_all_469_original_source_histories_replay_without_rescaling_or_date_shifts(self):
  rows=json.loads((R/'tests/fixtures/chart-defillama/data/histories.json').read_bytes());self.assertEqual(len(rows),469);self.assertEqual({r['id'] for r in rows},{d['id'] for d in m.CATALOGUE['series'].values()});total=0
  for r in rows:
   with self.subTest(id=r['id']):
    raw=gzip.decompress((R/r['path']).read_bytes());receipt=r['receipt'];self.assertEqual(hashlib.sha256(raw).hexdigest(),receipt['sha256']);self.assertEqual(len(raw),receipt['bytes']);original=json.loads(raw,parse_float=Decimal,parse_int=Decimal);self.assertEqual(len(original),r['rows'])
    expected=[[datetime.fromtimestamp(int(x['date']),timezone.utc).date().isoformat(),float(x['tvl'])] for x in original]
    p=m.fetch(r['id'],Store(),'test',reader=lambda url:(raw,{}) if url==receipt['url'] else self.fail('Source URL changed'),now=datetime.fromisoformat(receipt['received_at']))
    self.assertIsNone(p['quality']['error']);self.assertEqual(p['obs'],expected);self.assertEqual(p['n'],len(original));self.assertEqual(p['source'],receipt['url']);self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible'])
    extract=json.loads(gzip.decompress(base64.b64decode(p['source_extract']['body_base64'])));self.assertEqual([[int(x['date']),Decimal(x['tvl'])] for x in extract],[[int(x['date']),x['tvl']] for x in original]);total+=len(original)
  self.assertEqual(total,505324)
if __name__=='__main__':unittest.main(verbosity=2)
