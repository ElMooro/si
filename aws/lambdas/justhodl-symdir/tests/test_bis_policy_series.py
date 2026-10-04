import base64,csv,gzip,hashlib,io,json,sys,unittest,urllib.error
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'source'))
import bis_policy_series as b

FIELDS=['FREQ','REF_AREA','UNIT_MEASURE','UNIT_MULT','TIME_PERIOD','OBS_VALUE','OBS_STATUS','OBS_CONF','TITLE','COMPILATION']
def row(**kw):return dict(FREQ='M',REF_AREA='US',UNIT_MEASURE='368',UNIT_MULT='0',TIME_PERIOD='2025-01',OBS_VALUE='0',OBS_STATUS='A',OBS_CONF='F',TITLE='Invented policy rate',COMPILATION='Invented source',**kw)
def changed(**kw):r=row();r.update(kw);return r
def csv_bytes(rows):
 s=io.StringIO(newline='');w=csv.DictWriter(s,fieldnames=FIELDS);w.writeheader();w.writerows(rows);return s.getvalue().encode()
def packet(rows,**kw):return b.fetch('bis:WS_CBPOL:M.US',lambda _: (csv_bytes(rows),{}),**kw)

class PolicyTests(unittest.TestCase):
 def test_reviewed_catalog_and_twin(self):
  self.assertEqual(len(b.CATALOG['series']),98)
  for key,d in b.CATALOG['series'].items():
   self.assertEqual(b.definition('BIS:WS_CBPOL:'+key)['id'],'bis:WS_CBPOL:'+key)
   self.assertEqual(d['unit_code'],'368');self.assertEqual(d['unit_multiplier'],0)
  root=Path(b.__file__).resolve().parents[1]
  self.assertEqual((root/'source/bis-policy-series.json').read_bytes(),(root/'config/bis-policy-series.json').read_bytes())
 def test_exact_identity_only(self):
  for sid in ['bis:WS_CBPOL:M.USA','bis:WS_CBPOL:M.XX','bis:WS_CBPOL:*.US','bis:WS_CBPOL:M.US?x=1','bis:WS_CBPOL:M.US+GB','bis:WS_OTHER:M.US','bis:WS_CBPOL:M.US/../GB',None]:
   with self.subTest(sid=sid),self.assertRaises(ValueError):b.definition(sid)
 def test_original_bytes_dates_units_and_real_zero_preserved(self):
  raw=csv_bytes([row(),changed(TIME_PERIOD='2025-02',OBS_VALUE='-0.25'),changed(TIME_PERIOD='2025-03',OBS_VALUE='2.5',OBS_STATUS='P')]);d=b.fetch('bis:WS_CBPOL:M.US',lambda _:(gzip.compress(raw),{'Content-Encoding':'gzip'}))
  self.assertEqual(d['obs'],[['2025-01-01',0.0],['2025-02-01',-.25],['2025-03-01',2.5]])
  r=d['source_receipts'][0];self.assertEqual(gzip.decompress(base64.b64decode(r['body_base64'])),raw);self.assertEqual(r['sha256'],hashlib.sha256(raw).hexdigest())
  self.assertEqual(d['measurement_evidence']['rows'][1][1],'2025-02');self.assertIsNone(d['source_published_at']);self.assertFalse(d['calls_eligible']);self.assertFalse(d['sizing_eligible'])
 def test_suppression_confidentiality_status_and_non_numbers(self):
  cases=[{'OBS_VALUE':''},{'OBS_VALUE':'NaN'},{'OBS_VALUE':'false'},{'OBS_VALUE':'1e9999'},{'OBS_CONF':'C'},{'OBS_STATUS':'F'},{'OBS_STATUS':'M'},{'TIME_PERIOD':'2025-13'}]
  for case in cases:
   with self.subTest(case=case):self.assertEqual(packet([changed(**case)])['n'],0)
 def test_duplicate_periods_all_withheld(self):
  d=packet([row(),changed(OBS_VALUE='2')]);self.assertEqual(d['obs'],[['2025-01-01',None]]);self.assertEqual(d['quality']['rejected_rows'],2)
 def test_other_instrument_or_unit_withholds_whole_packet(self):
  for kw in [{'FREQ':'D'},{'REF_AREA':'GB'},{'UNIT_MEASURE':'628'},{'UNIT_MULT':'6'}]:
   d=packet([row(),changed(**kw)]);self.assertEqual(d['obs'],[]);self.assertFalse(d['history']['response_complete']);self.assertIsNotNone(d['quality']['error'])
 def test_daily_date_and_leap_day(self):
  rows=[changed(FREQ='D',TIME_PERIOD='2024-02-29'),changed(FREQ='D',TIME_PERIOD='2025-02-29')]
  d=b.fetch('bis:WS_CBPOL:D.US',lambda _:(csv_bytes(rows),{}));self.assertEqual(d['obs'],[['2024-02-29',0.0]]);self.assertEqual(d['quality']['rejected_rows'],1)
 def test_complete_shape_and_header_rejected(self):
  for raw in [b'FREQ,FREQ\nM,M\n',csv_bytes([row()]).replace(b'FREQ,',b'WRONG,'),csv_bytes([row()])+b'M,US\n',b'\xff']:
   d=b.fetch('bis:WS_CBPOL:M.US',lambda _:(raw,{}));self.assertEqual(d['n'],0);self.assertFalse(d['history']['response_complete'])
 def test_encoded_and_decoded_limits(self):
  with patch.object(b,'MAX_WIRE',12),self.assertRaises(ValueError):b.unpack(b'a'*13)
  with patch.object(b,'MAX_RAW',12),self.assertRaises(ValueError):b.unpack(gzip.compress(b'a'*13))
  with patch.object(b,'MAX_ROWS',1):self.assertEqual(packet([row(),changed(TIME_PERIOD='2025-02')])['n'],0)
 def test_no_retry_after_denial(self):
  calls=[]
  def denied(url):calls.append(url);raise urllib.error.HTTPError(url,403,'denied',{},None)
  d=b.fetch('bis:WS_CBPOL:M.US',denied);self.assertEqual(len(calls),1);self.assertEqual(d['source_receipts'][0]['http_status'],403);self.assertEqual(d['obs'],[])
 def test_cache_rejects_wrong_identity_definition_and_legacy(self):
  d=packet([row()]);self.assertTrue(b.cache_valid(d,d['id']))
  for key,value in [('definition_sha256','wrong'),('definition',{}),('contract','legacy'),('id','bis:WS_CBPOL:M.GB'),('history',{})]:
   other=dict(d);other[key]=value;self.assertFalse(b.cache_valid(other,d['id']))
 def test_directory_paginates_all_without_filling_nonexistent_countries(self):
  allrows=b.directory(limit=500)['rows'];self.assertEqual(len(allrows),98);self.assertEqual(b.directory(limit=2,offset=3)['rows'],allrows[3:5]);self.assertEqual(b.directory('Zimbabwe')['rows'],[])
  self.assertEqual(len(b.directory('United States')['rows']),2)
  for kw in [{'limit':0},{'offset':-1},{'limit':True}]:
   with self.assertRaises(ValueError):b.directory(**kw)

if __name__=='__main__':unittest.main()
