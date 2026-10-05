from pathlib import Path
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import base64,copy,gzip,hashlib,io,json,sys,unittest,urllib.error
import cboe_index as m
D=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-cboe/data';NOW=datetime(2026,10,5,3,tzinfo=timezone.utc)
class StoreError(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Store:
 def __init__(self):self.objects={};self.puts=[];self.deny=False;self.fail_retention=False
 def get_object(self,Bucket,Key):
  if self.deny:raise StoreError('AccessDenied')
  if Key not in self.objects:raise StoreError('NoSuchKey')
  return {'Body':io.BytesIO(self.objects[Key])}
 def put_object(self,**kw):
  if self.deny or (self.fail_retention and '/responses/' in kw['Key']):raise StoreError('AccessDenied')
  if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.objects:raise StoreError('PreconditionFailed')
  self.objects[kw['Key']]=kw['Body'];self.puts.append(kw['Key'])
class Tests(unittest.TestCase):
 def setUp(self):self.store=Store();self.calls=[]
 def reader(self,url):
  self.calls.append(url);symbol=next(s for s,d in m.CATALOGUE['series'].items() if d['source_url']==url);return (D/(symbol+'-chart.json')).read_bytes(),{'Content-Type':'application/json'}
 def fetch(self,symbol='VIX6M',**kw):return m.fetch('cboeindex:'+symbol,self.store,'fixture',reader=kw.pop('reader',self.reader),now=kw.pop('now',NOW),**kw)
 def test_all_exact_definitions_and_case_normalization(self):
  rows=m.directory(limit=500)['rows'];self.assertEqual(len(rows),77)
  for row in rows:self.assertEqual(m.definition(row['id'].lower())['id'],row['id']);self.assertFalse(row['live_history_verified'])
  for sid in ['CBOE:VIX6M','cboeindex:VIX6M.extra','cboeindex:VX1!','cboeindex:UNKNOWN']:
   with self.assertRaises(ValueError):m.fetch(sid,self.store,'fixture',reader=self.reader,now=NOW)
  self.assertEqual(self.calls,[])
 def test_all_received_close_values_and_raw_records_preserved(self):
  for symbol in m.CATALOGUE['series']:
   p=self.fetch(symbol);raw=(D/(symbol+'-chart.json')).read_bytes();original=json.loads(raw);self.assertIsNone(p['quality']['error']);self.assertEqual(p['obs'],[[r['date'],float(Decimal(r['close']))] for r in original['data']]);self.assertEqual(p['n'],len(original['data']))
   x=p['source_extract'];extract=gzip.decompress(base64.b64decode(x['body_base64']));self.assertEqual(json.loads(extract),original['data']);self.assertEqual(hashlib.sha256(extract).hexdigest(),x['sha256']);self.assertEqual(self.store.objects[p['source_receipts'][0]['retained_key']],raw)
   self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertFalse(p['history']['market_ohlc_qualified']);self.assertFalse(p['history']['traded_volume_qualified']);self.assertIsNone(p['source_generated_timezone']);self.assertEqual(p['source_generated_at_raw'],original['timestamp']);self.assertTrue(m.cache_valid(p,p['id']))
 def test_malformed_ohlc_never_becomes_candles_or_invented_extremes(self):
  for symbol,n in [('VIX6M',1),('BXN',1),('SET',182),('OET',178)]:
   p=self.fetch(symbol);self.assertEqual(p['quality']['source_flag_counts']['source_ohlc_inconsistent'],n);self.assertEqual(p['quality']['status'],'partial');self.assertEqual(p['quality']['rejected_rows'],0);self.assertNotIn('bars',p)
 def test_settlement_and_cash_volatility_are_different_definitions(self):
  self.assertEqual(m.definition('cboeindex:VRO')['measurement_kind'],'settlement_value');self.assertEqual(m.definition('cboeindex:VIX6M')['measurement_kind'],'volatility_index');self.assertEqual(m.definition('cboeindex:SET')['measurement_kind'],'settlement_value')
 def test_missing_duplicate_dates_and_precision_are_explicit(self):
  p=json.loads((D/'VIX6M-chart.json').read_bytes());row=p['data'][0];p['data']=[dict(row,close=''),dict(row,date='2026-01-02',close='0'),dict(row,date='2026-01-03',close='1.1234567890123456789'),dict(row,date='2026-01-04',close='3'),dict(row,date='2026-01-04',close='4')]
  obs,records,_=m.parsed(json.dumps(p).encode(),m.definition('cboeindex:VIX6M'));self.assertEqual(obs[0][1],None);self.assertEqual(obs[1][1],0);self.assertTrue(records[2]['binary64_rounding']);self.assertEqual(obs[-1][1],None);self.assertTrue(all(r['rejection']=='duplicate_reference_date' for r in records[-2:]));self.assertEqual(records[2]['original']['close'],'1.1234567890123456789')
 def test_wrong_identity_schema_duplicate_json_or_partial_body_cannot_be_retained(self):
  source=json.loads((D/'VIX6M-chart.json').read_bytes())
  variants=[dict(source,symbol='_VXD'),dict(source,other='unexpected'),dict(source,data=[{'date':'2026-01-01','close':'1'}])]
  raws=[json.dumps(p).encode() for p in variants]+[b'{"symbol":"_VIX6M","symbol":"_VIX6M"}',b'{"data":']
  for raw in raws:
   self.store=Store();p=self.fetch(reader=lambda _: (raw,{}));self.assertEqual(p['n'],0);self.assertFalse(any('/responses/' in k for k in self.store.objects))
 def test_refusal_blocks_other_symbols_and_never_retries(self):
  for code in [401,403,429]:
   self.store=Store();calls=[]
   def denied(url):calls.append(url);raise urllib.error.HTTPError(url,code,'Denied',{},None)
   p=self.fetch(reader=denied);self.assertEqual(p['n'],0);self.fetch('VXD',reader=denied);self.assertEqual(len(calls),1);self.assertIn(m.ROOT+'blocked.json',self.store.objects)
 def test_non_access_http_failure_stays_per_symbol(self):
  def missing(url):raise urllib.error.HTTPError(url,404,'Missing',{},None)
  self.assertEqual(self.fetch(reader=missing)['n'],0);self.assertGreater(self.fetch('VXD')['n'],0);self.assertNotIn(m.ROOT+'blocked.json',self.store.objects)
 def test_shared_cache_and_daily_claim_retain_whole_original(self):
  first=self.fetch();second=self.fetch(now=NOW+timedelta(minutes=10));self.assertEqual(first['obs'],second['obs']);self.assertEqual(len(self.calls),1)
  key=m.prefix(m.definition('cboeindex:VIX6M'));self.store.objects.pop(key+'current.json');third=self.fetch();self.assertEqual(third['n'],0);self.assertEqual(len(self.calls),1)
 def test_retention_or_cache_failure_does_not_serve_untraceable_values(self):
  self.store.fail_retention=True;self.assertEqual(self.fetch()['n'],0);self.store=Store();self.store.deny=True;self.calls=[];self.assertEqual(self.fetch()['n'],0);self.assertEqual(self.calls,[])
 def test_receipt_corruption_and_future_clock_fail_closed(self):
  self.fetch();key=m.prefix(m.definition('cboeindex:VIX6M'))+'current.json';r=json.loads(self.store.objects[key]);r['bytes']+=1;self.store.objects[key]=json.dumps(r).encode();self.assertEqual(self.fetch()['n'],0);self.assertEqual(len(self.calls),1)
  self.store=Store();self.fetch();self.assertEqual(self.fetch(now=NOW-timedelta(minutes=1))['n'],0)
 def test_actual_zero_remains_zero_and_nonfinite_values_never_plot(self):
  for raw in ['',None,True,'NaN','Infinity','1e9999','1e-9999']:self.assertIsNone(m.measured(raw))
  self.assertEqual(m.measured('0.000000'),0);self.assertEqual(m.measured('-1.50'),-1.5)
if __name__=='__main__':unittest.main(verbosity=2)
