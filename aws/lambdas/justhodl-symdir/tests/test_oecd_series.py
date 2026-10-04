import base64,csv,gzip,io,json,sys,unittest,urllib.error
from datetime import datetime,timedelta,timezone
from unittest.mock import patch
import oecd_series as m

class StoreError(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Store:
 def __init__(self):self.objects={};self.puts=[];self.deny=False
 def get_object(self,Bucket,Key):
  if self.deny:raise StoreError('AccessDenied')
  if Key not in self.objects:raise StoreError('NoSuchKey')
  return {'Body':io.BytesIO(self.objects[Key])}
 def put_object(self,**kw):
  if self.deny:raise StoreError('AccessDenied')
  if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.objects:raise StoreError('PreconditionFailed')
  self.objects[kw['Key']]=kw['Body'];self.puts.append(kw['Key'])

SID='oecd:'+m.FLOW+':USA.M.PRVM.IX.BTE.Y._Z._Z.N'
NOW=datetime(2026,10,4,12,tzinfo=timezone.utc)
def raw(rows=None,**change):
 d=m.definition(SID);r=dict(DATAFLOW=m.CATALOG['dataflow'],**d['dimensions'],TIME_PERIOD='2026-01',OBS_VALUE='100.5',OBS_STATUS='A',UNIT_MULT='0',DECIMALS='2',BASE_PER='2015');r.update(change)
 out=io.StringIO(newline='');w=csv.DictWriter(out,fieldnames=list(r),lineterminator='\n');w.writeheader();w.writerows(rows if rows is not None else [r]);return out.getvalue().encode()
class Tests(unittest.TestCase):
 def setUp(self):m._memory_source=None;m._memory_index=None;self.s=Store();self.calls=0
 def reader(self,url):self.calls+=1;self.assertEqual(url,m.SOURCE_URL);return gzip.compress(raw()),{'Content-Type':'text/csv'}
 def fetch(self,**kwargs):return m.fetch(SID,self.s,'fixture',reader=self.reader,now=NOW,**kwargs)
 def test_exact_definitions_and_all_directory_pages(self):
  ids=[]
  for offset in range(0,1511,500):ids += [r['id'] for r in m.directory(limit=500,offset=offset)['rows']]
  self.assertEqual(len(ids),1511);self.assertEqual(len(set(ids)),1511);self.assertEqual(m.definition(SID.upper())['id'],SID)
  for sid in ['oecd:DSD_STES@DF_INDSERV','oecd:'+m.FLOW+':unknown',SID.replace('4.3','4.2'),SID+':GARBAGE','https://evil.test']:
   with self.assertRaises(ValueError):m.definition(sid)
  self.assertEqual(self.calls,0)
 def test_shared_gate_retained_response_and_exact_replay(self):
  p=self.fetch();q=self.fetch();self.assertEqual(self.calls,1);self.assertEqual(p['obs'],[['2026-01-01',100.5]]);self.assertEqual(p['obs'],q['obs'])
  r=p['source_receipts'][0];self.assertEqual(gzip.decompress(self.s.objects[r['retained_key']]),raw())
  extracted=gzip.decompress(base64.b64decode(p['source_extract']['body_base64']));self.assertEqual(extracted,raw())
  self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertFalse(p['history']['full_upstream_history_verified']);self.assertTrue(m.cache_valid(p,SID))
 def test_unavailable_shared_store_never_downloads(self):
  self.s.deny=True;p=self.fetch();self.assertEqual(p['n'],0);self.assertEqual(self.calls,0);self.assertFalse(m.cache_valid(p,SID))
 def test_existing_claim_without_response_does_not_retry(self):
  self.s.objects[m.PREFIX+'download-claims/'+NOW.date().isoformat()+'.json']=b'{}';self.assertEqual(self.fetch()['n'],0);self.assertEqual(self.calls,0)
 def test_http_denial_persists_stop_even_next_day(self):
  def denied(url):self.calls+=1;raise urllib.error.HTTPError(url,403,'denied',{},None)
  p=m.fetch(SID,self.s,'fixture',reader=denied,now=NOW);q=m.fetch(SID,self.s,'fixture',reader=denied,now=NOW+timedelta(days=2))
  self.assertEqual(self.calls,1);self.assertEqual(p['n'],0);self.assertEqual(q['n'],0);self.assertIn(m.PREFIX+'blocked.json',self.s.objects)
 def test_wrong_units_or_flow_are_never_accepted(self):
  for fields in [{'BASE_PER':'2020'},{'UNIT_MULT':'3'},{'DATAFLOW':'other'},{'OBS_VALUE':'NaN'}]:
   m._memory_index=None
   if 'OBS_VALUE' in fields:self.assertEqual(m.parse(raw(**fields),m.definition(SID))[0],[['2026-01-01',None]])
   else:
    with self.assertRaises(ValueError):m.parse(raw(**fields),m.definition(SID))
 def test_missing_confidential_and_duplicate_rows_are_null(self):
  d=m.definition(SID)
  for status in ('M','C','S','unknown'):
   obs,rows,_=m.parse(raw(OBS_STATUS=status),d);self.assertEqual(obs,[['2026-01-01',None]]);self.assertTrue(rows[0]['rejection'])
  r=next(csv.DictReader(io.StringIO(raw().decode())));obs,rows,_=m.parse(raw([r,r]),d);self.assertEqual(obs,[['2026-01-01',None]]);self.assertTrue(all(x['rejection']=='duplicate_reference_period' for x in rows))
 def test_zero_is_a_real_value_and_breaks_are_retained(self):
  obs,rows,_=m.parse(raw(OBS_VALUE='0',OBS_STATUS='B'),m.definition(SID));self.assertEqual(obs,[['2026-01-01',0.0]]);self.assertEqual(rows[0]['original']['OBS_STATUS'],'B')
 def test_tampered_retained_response_is_rejected(self):
  p=self.fetch();key=p['source_receipts'][0]['retained_key'];self.s.objects[key]=b'bad';m._memory_source=None
  q=self.fetch();self.assertEqual(q['n'],0);self.assertEqual(self.calls,1)
 def test_stale_snapshot_is_never_fresh_cache(self):
  self.fetch();future=NOW+timedelta(days=2);self.s.objects[m.PREFIX+'download-claims/'+future.date().isoformat()+'.json']=b'{}'
  p=m.fetch(SID,self.s,'fixture',reader=self.reader,now=future);self.assertEqual(self.calls,1);self.assertEqual(p['quality']['status'],'stale_source_snapshot');self.assertFalse(m.cache_valid(p,SID));self.assertTrue(p['history']['source_snapshot_stale'])
 def test_unknown_series_does_not_touch_store_or_reader(self):
  self.s.deny=True
  with self.assertRaises(ValueError):m.fetch(SID.replace('USA','ZZZ'),self.s,'fixture',reader=self.reader)
  self.assertEqual(self.calls,0)
 def test_no_redirect_no_ssrf_and_period_anchors(self):
  with self.assertRaises(ValueError):m.read_http('https://evil.test')
  with self.assertRaises(urllib.error.HTTPError):m.NoRedirect().redirect_request(urllib.request.Request(m.SOURCE_URL),None,302,'moved',{},'https://evil.test')
  self.assertEqual(m.period('2026-Q3','Q'),'2026-07-01');self.assertIsNone(m.period('2026-13','M'));self.assertEqual(m.period('2026','A'),'2026-01-01')
 def test_growth_uses_calendar_lag_not_row_count(self):
  r=next(csv.DictReader(io.StringIO(raw().decode())))
  rows=[dict(r,TIME_PERIOD='2025-01',OBS_VALUE='100'),dict(r,TIME_PERIOD='2025-03',OBS_VALUE='999'),dict(r,TIME_PERIOD='2026-01',OBS_VALUE='110')]
  obs,records,_=m.parse(raw(rows),m.definition(SID));out,evidence=m.growth(obs,records,12)
  self.assertEqual(out[-1],['2026-01-01',10.0]);self.assertEqual(evidence[-1][1],'2025-01-01');self.assertIsNone(out[1][1])
 def test_growth_break_and_zero_denom_are_withheld(self):
  r=next(csv.DictReader(io.StringIO(raw().decode())))
  for variant,reason in [('break','source_break_inside_comparison_window'),('zero','zero_denominator')]:
   rows=[dict(r,TIME_PERIOD='2025-01',OBS_VALUE='0' if variant=='zero' else '100'),dict(r,TIME_PERIOD='2025-03',OBS_VALUE='90',OBS_STATUS='B' if variant=='break' else 'A'),dict(r,TIME_PERIOD='2026-01',OBS_VALUE='110')]
   obs,records,_=m.parse(raw(rows),m.definition(SID));out,evidence=m.growth(obs,records,12);self.assertIsNone(out[-1][1]);self.assertEqual(evidence[-1][-1],reason)
 def test_failed_transform_cannot_return_levels_as_percent(self):
  with patch.object(m,'growth',side_effect=ValueError('bad transform')):
   p=m.fetch(SID+':GY',self.s,'fixture',reader=self.reader,now=NOW)
  self.assertEqual(p['n'],0);self.assertEqual(p['obs'],[]);self.assertIn('bad transform',p['quality']['error'])
 def test_growth_is_explicitly_computed_and_not_annualised(self):
  r=next(csv.DictReader(io.StringIO(raw().decode())))
  rows=[dict(r,TIME_PERIOD='2026-01',OBS_VALUE='100'),dict(r,TIME_PERIOD='2026-02',OBS_VALUE='110')]
  p=m.fetch(SID+':G1',self.s,'fixture',reader=lambda u:(raw(rows),{}),now=NOW)
  self.assertEqual(p['obs'],[['2026-01-01',None],['2026-02-01',10.0]]);self.assertEqual(p['transformation']['computed_by'],'JustHodl');self.assertFalse(p['transformation']['provider_published_growth']);self.assertFalse(p['transformation']['annualised'])
  self.assertFalse(m.cache_valid(p,SID));self.assertTrue(m.cache_valid(p,SID+':G1'))
if __name__=='__main__':unittest.main()
