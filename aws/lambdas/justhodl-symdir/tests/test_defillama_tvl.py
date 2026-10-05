from pathlib import Path
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import base64,copy,gzip,hashlib,io,json,unittest,urllib.error
import defillama_tvl as m
F=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-defillama/data'
RAW=gzip.decompress((F/'total-tvl.json.gz').read_bytes());NOW=datetime(2026,10,5,4,tzinfo=timezone.utc);SID='defillama:tvl:all'
class StoreError(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Store:
 def __init__(self):self.objects={};self.puts=[];self.deny=False;self.fail_retention=False
 def get_object(self,Bucket,Key):
  if self.deny:raise StoreError('AccessDenied')
  if Key not in self.objects:raise StoreError('NoSuchKey')
  return {'Body':io.BytesIO(self.objects[Key])}
 def put_object(self,**kw):
  if self.deny or self.fail_retention and '/responses/' in kw['Key']:raise StoreError('AccessDenied')
  if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.objects:raise StoreError('PreconditionFailed')
  self.objects[kw['Key']]=kw['Body'];self.puts.append(kw['Key'])
class Tests(unittest.TestCase):
 def setUp(self):self.store=Store();self.calls=[]
 def reader(self,url):self.calls.append(url);return RAW,{'Content-Type':'application/json'}
 def fetch(self,sid=SID,**kw):return m.fetch(sid,self.store,'fixture',reader=kw.pop('reader',self.reader),now=kw.pop('now',NOW),**kw)
 def test_catalogue_exactness_and_no_chain_or_token_guessing(self):
  rows=m.directory(limit=500)['rows'];self.assertEqual(len(rows),469)
  for r in rows:self.assertEqual(m.definition(r['id'].upper())['id'],r['id']);self.assertIsNone(r['n']);self.assertIsNone(r['first']);self.assertIsNone(r['last']);self.assertFalse(r['live_history_verified']);self.assertEqual(r['unit'],'USD')
  for sid in ['DEFILLAMA:TOTAL_TVL','defillama:tvl:ETH','defillama:tvl:Ethereum?x=1','defillama:tvl:../all','defillama:tvl:all:extra']:
   with self.assertRaises(ValueError):self.fetch(sid)
  self.assertEqual(self.calls,[])
 def test_whole_public_total_history_independently_reconstructed(self):
  p=self.fetch();rows=json.loads(RAW,parse_float=Decimal,parse_int=Decimal)
  expected=[[datetime.fromtimestamp(int(r['date']),timezone.utc).date().isoformat(),float(r['tvl'])] for r in rows]
  self.assertEqual(p['obs'],expected);self.assertEqual(p['n'],len(rows));self.assertEqual(len(rows),3296)
  x=p['source_extract'];ex=gzip.decompress(base64.b64decode(x['body_base64']));self.assertEqual(hashlib.sha256(ex).hexdigest(),x['sha256']);self.assertEqual(len(ex),x['bytes'])
  original=json.loads(ex);self.assertEqual([Decimal(r['tvl']) for r in original],[r['tvl'] for r in rows]);self.assertEqual([int(r['date']) for r in original],[int(r['date']) for r in rows]);self.assertEqual(self.store.objects[p['source_receipts'][0]['retained_key']],RAW)
  self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertFalse(p['history']['market_ohlc_qualified']);self.assertFalse(p['history']['traded_volume_qualified']);self.assertFalse(p['history']['completed_day_verified']);self.assertFalse(p['history']['release_clock_verified']);self.assertTrue(m.cache_valid(p,SID));self.assertIn('not net deposits',p['history']['interpretation'])
 def test_missing_invalid_scalars_are_not_zero_and_original_lexemes_survive(self):
  raw=b'[{"date":1767225600,"tvl":null},{"date":1767312000,"tvl":0},{"date":1767398400,"tvl":1.1234567890123456789},{"date":1767484800,"tvl":true},{"date":1767571200,"tvl":"123"},{"date":1767657600,"tvl":-1}]'
  obs,records,_=m.parsed(raw,m.definition(SID));self.assertEqual([v for _,v in obs],[None,0,1.1234567890123457,None,None,None]);self.assertTrue(records[2]['binary64_rounding']);self.assertEqual(records[2]['original']['tvl'],'1.1234567890123456789');self.assertEqual(records[-1]['rejection'],'negative_tvl_requires_source_review')
 def test_duplicate_days_are_withheld_without_erasing_rows(self):
  obs,rows,_=m.parsed(b'[{"date":1767225600,"tvl":1},{"date":1767225600,"tvl":1}]',m.definition(SID));self.assertEqual(obs,[['2026-01-01',None]]);self.assertEqual(len(rows),2);self.assertTrue(all(r['rejection']=='duplicate_reference_date' for r in rows))
 def test_intraday_string_boolean_and_invalid_timestamps_cannot_be_rounded_to_days(self):
  for date in [1767225601,'1767225600',True,None,-1,1e20]:
   obs,rows,_=m.parsed(json.dumps([{'date':date,'tvl':3}]).encode(),m.definition(SID));self.assertEqual(obs,[]);self.assertEqual(rows[0]['rejection'],'invalid_or_non_midnight_reference_time')
 def test_schema_and_partial_response_rejected_without_retention(self):
  for raw in [b'[{"date":1,"date":2,"tvl":1}]',b'[{"date":1767225600,"tvl":NaN}]',b'[{"date":1767225600,"tvl":1,"other":0}]',b'{"data":[]}',b'[']:
   self.store=Store();p=self.fetch(reader=lambda _:(raw,{}));self.assertEqual(p['n'],0);self.assertFalse(any('/responses/' in k for k in self.store.objects));self.assertFalse(p['history']['response_complete'])
 def test_future_source_dates_remain_in_evidence_but_never_plot_as_history(self):
  timestamp=int(datetime(2027,1,1,tzinfo=timezone.utc).timestamp());p=self.fetch(reader=lambda _:(json.dumps([{'date':timestamp,'tvl':100}]).encode(),{}))
  self.assertEqual(p['obs'],[['2027-01-01',None]]);self.assertEqual(p['n'],0);self.assertEqual(p['measurement_evidence']['rows'][0][5],'future_reference_date_at_receipt');self.assertEqual(p['measurement_evidence']['rows'][0][4],'100');self.assertTrue(p['history']['response_complete'])
 def test_empty_is_unavailable_not_fabricated_zero(self):
  p=self.fetch(reader=lambda _:(b'[]',{}));self.assertEqual(p['obs'],[]);self.assertEqual(p['n'],0);self.assertEqual(p['quality']['status'],'unavailable');self.assertTrue(p['history']['response_complete']);self.assertEqual(p['quality']['received_rows'],0)
 def test_access_refusals_stop_all_chains_without_retry(self):
  for code in [401,403,429]:
   self.store=Store();calls=[]
   def denied(url):calls.append(url);raise urllib.error.HTTPError(url,code,'Denied',{},None)
   self.assertEqual(self.fetch(reader=denied)['n'],0);self.assertEqual(self.fetch('defillama:tvl:Ethereum',reader=denied,now=NOW+timedelta(seconds=4))['n'],0);self.assertEqual(len(calls),1);self.assertIn(m.ROOT+'blocked.json',self.store.objects)
 def test_other_http_failure_stays_per_definition(self):
  def missing(url):raise urllib.error.HTTPError(url,404,'Missing',{},None)
  self.assertEqual(self.fetch(reader=missing)['n'],0);self.assertGreater(self.fetch('defillama:tvl:Ethereum',now=NOW+timedelta(seconds=4))['n'],0);self.assertNotIn(m.ROOT+'blocked.json',self.store.objects)
 def test_shared_two_second_slot_never_consumes_busy_series_daily_claim(self):
  self.fetch();other='defillama:tvl:Ethereum';self.assertEqual(self.fetch(other)['n'],0);self.assertEqual(len(self.calls),1);self.assertFalse(any(m.prefix(m.definition(other))+'request-claims/' in k for k in self.store.objects));self.assertGreater(self.fetch(other,now=NOW+timedelta(seconds=4))['n'],0);self.assertEqual(len(self.calls),2)
 def test_shared_cache_and_daily_claim_survive_loss_of_current_pointer(self):
  first=self.fetch();second=self.fetch(now=NOW+timedelta(minutes=10));self.assertEqual(first['obs'],second['obs']);self.assertEqual(len(self.calls),1);key=m.prefix(m.definition(SID));self.store.objects.pop(key+'current.json');self.assertEqual(self.fetch(now=NOW+timedelta(hours=1))['n'],0);self.assertEqual(len(self.calls),1)
 def test_retention_cache_or_receipt_failure_never_serves_untraceable_values(self):
  self.store.fail_retention=True;self.assertEqual(self.fetch()['n'],0);self.store=Store();self.store.deny=True;self.calls=[];self.assertEqual(self.fetch()['n'],0);self.assertEqual(self.calls,[])
  self.store=Store();self.fetch();key=m.prefix(m.definition(SID))+'current.json';r=json.loads(self.store.objects[key]);r['bytes']+=1;self.store.objects[key]=json.dumps(r).encode();self.assertEqual(self.fetch()['n'],0)
 def test_definition_change_invalidates_receipt_and_legacy_cache(self):
  p=self.fetch();d=m.definition(SID);other=dict(d,unit='BTC')
  self.assertFalse(m.receipt_definition_matches(p['source_receipts'][0],other));self.assertFalse(m.cache_valid(dict(p,definition=other),SID));self.assertFalse(m.cache_valid({'id':SID,'n':2},SID))
 def test_clocks_are_not_inferred_or_shifted(self):
  self.fetch();self.assertEqual(self.fetch(now=NOW-timedelta(seconds=1))['n'],0)
  with self.assertRaises(ValueError):m.snapshot(m.definition(SID),self.store,'fixture',self.reader,datetime(2026,10,5))
 def test_underflow_nonfinite_and_boolean_scalars_rejected(self):
  for value in [None,True,False,[],{},'123',m.NumericLexeme('1e9999'),m.NumericLexeme('1e-9999'),m.NumericLexeme('NaN')]:self.assertIsNone(m.measured(value))
  self.assertEqual(m.measured(m.NumericLexeme('0')),0);self.assertEqual(m.measured(m.NumericLexeme('1.25')),1.25)
if __name__=='__main__':unittest.main(verbosity=2)
