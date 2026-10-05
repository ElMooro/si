from pathlib import Path
import gzip,json,unittest
import regional_fed as m
from test_regional_fed import Store,Reader,NOW
F=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-regional-fed/data'
class Tests(unittest.TestCase):
 def test_all_77_histories_match_independent_source_workbook_replay(self):
  expected=json.loads(gzip.decompress((F/'independent-expected.json.gz').read_bytes()));self.assertEqual(set(expected),{sid for sid,d in m.CATALOGUE['series'].items() if d['dataset'] in ('chicago-cfnai','kc-manufacturing')});store=Store();reader=Reader()
  for sid,obs in expected.items():
   p=m.fetch(sid,store,'invented-offline-bucket',reader,NOW);self.assertIsNone(p['quality']['error']);self.assertEqual(p['obs'],obs,sid);self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible'])
  self.assertEqual(len(reader.urls),3)
if __name__=='__main__':unittest.main(verbosity=2)
