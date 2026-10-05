from pathlib import Path
from datetime import datetime,timedelta,timezone
import base64,csv,gzip,hashlib,io,json,unittest,urllib.error,urllib.request,zipfile
from unittest.mock import patch
import census_series as m

F=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-census/data'
MANIFEST=json.loads((F/'manifest.json').read_bytes())
NOW=datetime(2026,10,4,12,tzinfo=timezone.utc)
SID=MANIFEST['mrts']['series'][0]

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

def archive(slug='mrts',raw=None,readme=None,extra=False):
 stream=io.BytesIO()
 with zipfile.ZipFile(stream,'w',zipfile.ZIP_DEFLATED) as z:
  z.writestr(slug.upper()+'-mf.csv',(F/(slug+'.csv')).read_bytes() if raw is None else raw)
  z.writestr('/README',(F/'README-source.txt').read_bytes() if readme is None else readme)
  if extra:z.writestr('../../escape','bad')
 return stream.getvalue()

def mutate_records(mutator,slug='mrts'):
 rows=list(csv.reader(io.StringIO((F/(slug+'.csv')).read_text(encoding='utf-8'),newline='')))
 at=rows.index(['DATA'])+2;data=[row for row in rows[at:] if row];new=mutator(data)
 out=io.StringIO(newline='');writer=csv.writer(out,lineterminator='\n');writer.writerows(rows[:at]+new);return out.getvalue().encode()

class Tests(unittest.TestCase):
 def setUp(self):m._memory_source.clear();m._memory_index.clear();self.store=Store();self.calls=[]
 def reader(self,url):
  self.calls.append(url);slug=next(k for k,v in m.CATALOGUE['dataset_definitions'].items() if v['source_url']==url)
  return archive(slug),{'Content-Type':'application/zip'}
 def fetch(self,sid=SID,**kw):return m.fetch(sid,self.store,'fixture',reader=kw.pop('reader',self.reader),now=kw.pop('now',NOW),**kw)
 def test_all_exact_series_definitions_and_pages(self):
  ids=[]
  for pos in range(0,3402,500):ids.extend(r['id'] for r in m.directory(limit=500,offset=pos)['rows'])
  self.assertEqual(len(ids),3402);self.assertEqual(len(set(ids)),3402)
  for sid in ids:self.assertEqual(m.definition(sid.upper())['id'],sid)
  self.assertEqual(m.directory(dataset='qss',limit=1)['total'],2144)
 def test_canonical_identity_rejects_extra_dimensions_or_guessing(self):
  for sid in [SID+':extra',SID.replace(':no:',':unknown:'),'census:mrts:SM','census:unknown:SM:TOTAL:no:US','https://evil.test']:
   with self.assertRaises(ValueError):self.fetch(sid)
  self.assertEqual(self.calls,[]);self.assertEqual(self.store.puts,[])
 def test_original_zip_retained_and_extract_replays(self):
  p=self.fetch();q=self.fetch();self.assertEqual(len(self.calls),1);self.assertEqual(p['n'],3);self.assertEqual(p['obs'],q['obs'])
  r=p['source_receipts'][0];wire=self.store.objects[r['retained_key']]
  self.assertEqual(hashlib.sha256(wire).hexdigest(),r['zip_sha256']);self.assertEqual(m.unpack(wire,'mrts')[0],(F/'mrts.csv').read_bytes())
  raw=gzip.decompress(base64.b64decode(p['source_extract']['body_base64']));self.assertEqual(len(list(csv.DictReader(io.StringIO(raw.decode())))),3)
  self.assertEqual(hashlib.sha256(raw).hexdigest(),p['source_extract']['sha256']);self.assertTrue(m.cache_valid(p,SID))
  self.assertFalse(p['calls_eligible']);self.assertFalse(p['history']['full_upstream_history_verified'])
 def test_all_six_datasets_use_separate_source_and_gate(self):
  for slug,row in MANIFEST.items():
   p=self.fetch(row['series'][0]);self.assertEqual(p['n'],3);self.assertEqual(p['freq'],'Q' if slug=='qss' else 'M')
  self.assertEqual(len(self.calls),6);self.assertEqual(len(set(self.calls)),6)
 def test_sampling_error_stays_separate(self):
  for slug,row in MANIFEST.items():
   p=self.fetch(row['series'][1]);self.assertTrue(p['definition']['sampling_error']);self.assertEqual(p['n'],3)
   self.assertNotEqual(p['id'],row['series'][0]);self.assertIn('err_desc',p['definition']['measure_definition'])
 def test_markers_are_null_but_numeric_zero_stays_zero(self):
  for marker,reason in [('Z','estimate_rounds_to_zero'),('S','source_suppressed'),('(S)','source_suppressed'),('NaN','source_unavailable_or_unrepresentable_value')]:
   raw=mutate_records(lambda rows:[[ *r[:-1],marker] if i==0 else r for i,r in enumerate(rows)])
   obs,records,_,_=m.parse(raw,m.definition(SID));self.assertIsNone(obs[0][1]);self.assertEqual(records[0]['rejection'],reason)
  raw=mutate_records(lambda rows:[[*r[:-1],'0'] if i==0 else r for i,r in enumerate(rows)])
  self.assertEqual(m.parse(raw,m.definition(SID))[0][0][1],0)
 def test_duplicate_period_is_null_with_both_records_preserved(self):
  raw=mutate_records(lambda rows:rows+[rows[0]]);obs,rows,_,_=m.parse(raw,m.definition(SID))
  self.assertIsNone(obs[0][1]);self.assertEqual(sum(r['rejection']=='duplicate_reference_period' for r in rows),2)
 def test_frequency_anchor_not_daily_or_release_date(self):
  for text,expected in [('Jan-2020',('2020-01-01','M')),('Q4-2020',('2020-10-01','Q')),('2020',('2020-01-01','A')),('Bad', (None,None))]:self.assertEqual(m.period(text),expected)
  p=self.fetch(MANIFEST['qss']['series'][0]);self.assertEqual(p['freq'],'Q');self.assertIsNone(p['source_published_at']);self.assertFalse(p['history']['release_clock_verified'])
 def test_missing_dictionary_reference_and_invalid_adjustment_fail(self):
  for column,value in [(1,'999999'),(5,'2'),(2,'0')]:
   raw=mutate_records(lambda rows:[[value if j==column else v for j,v in enumerate(r)] if i==0 else r for i,r in enumerate(rows)])
   with self.assertRaises(ValueError):m.parse(raw,m.definition(SID))
 def test_changed_measure_units_require_review(self):
  raw=(F/'mrts.csv').read_bytes().replace(b'MLN$',b'BLN$')
  with self.assertRaisesRegex(ValueError,'changed'):m.parse(raw,m.definition(SID))
 def test_unrepresentable_numeric_value_is_not_rounded(self):
  for value in ['9007199254740993','1e999','nan','true','1,200','']:self.assertIsNone(m.measured(value))
  self.assertIsNone(m.measured(True));self.assertEqual(m.measured('1.20'),1.2)
 def test_changed_readme_and_unexpected_zip_members_refuse_history(self):
  for kwargs in [{'readme':b'changed units'},{'extra':True}]:
   p=self.fetch(reader=lambda u:(archive(**kwargs),{}));self.assertEqual(p['n'],0);self.assertFalse(p['history']['response_complete']);self.setUp()
 def test_shared_store_denial_does_not_download(self):
  self.store.deny=True;p=self.fetch();self.assertEqual(p['n'],0);self.assertEqual(self.calls,[])
 def test_retention_failure_does_not_serve_untraceable_history(self):
  self.store.fail_retention=True;p=self.fetch();self.assertEqual(p['n'],0);self.assertFalse(m.cache_valid(p,SID))
 def test_claim_without_response_stops_repeated_download(self):
  self.store.objects[m.prefix('mrts')+'download-claims/'+NOW.date().isoformat()+'.json']=b'{}';self.assertEqual(self.fetch()['n'],0);self.assertEqual(self.calls,[])
 def test_http_refusal_persists_across_days(self):
  def denied(url):self.calls.append(url);raise urllib.error.HTTPError(url,403,'denied',{},None)
  self.assertEqual(self.fetch(reader=denied)['n'],0);self.assertEqual(self.fetch(reader=denied,now=NOW+timedelta(days=2))['n'],0);self.assertEqual(len(self.calls),1)
 def test_stale_snapshot_has_explicit_label_and_cannot_pass_cache(self):
  self.fetch();future=NOW+timedelta(days=2);self.store.objects[m.prefix('mrts')+'download-claims/'+future.date().isoformat()+'.json']=b'{}'
  p=self.fetch(now=future);self.assertEqual(p['n'],3);self.assertEqual(p['quality']['status'],'stale_source_snapshot');self.assertFalse(m.cache_valid(p,SID));self.assertEqual(len(self.calls),1)
 def test_future_receipt_clock_is_not_fresh(self):
  self.fetch();p=self.fetch(now=NOW-timedelta(days=1));self.assertEqual(p['n'],0);self.assertEqual(len(self.calls),1)
 def test_tampered_archive_or_receipt_is_rejected(self):
  for variant in ['archive','receipt']:
   p=self.fetch();r=p['source_receipts'][0]
   if variant=='archive':self.store.objects[r['retained_key']]=b'bad'
   else:
    r['csv_sha256']='0'*64;self.store.objects[m.prefix('mrts')+'current.json']=json.dumps(r).encode()
   m._memory_source.clear();self.assertEqual(self.fetch()['n'],0);self.assertEqual(len(self.calls),1);self.setUp()
 def test_no_redirect_no_ssrf_or_data_api(self):
  for url in ['https://evil.test','https://api.census.gov/data/timeseries/eits/mrts']:
   with self.assertRaises(ValueError):m.read_http(url)
  with self.assertRaises(urllib.error.HTTPError):m.NoRedirect().redirect_request(urllib.request.Request(m.CATALOGUE['dataset_definitions']['mrts']['source_url']),None,302,'moved',{},'https://evil.test')
 def test_catalogue_counts_do_not_claim_numeric_or_full_history(self):
  rows=[]
  for pos in range(0,3402,500):rows+=m.directory(limit=500,offset=pos)['rows']
  self.assertEqual(sum(r['snapshot_numeric_rows']==0 for r in rows),4)
  self.assertTrue(all(r['n'] is None and r['live_history_verified'] is False for r in rows))
 def test_invalid_page_bounds(self):
  for args in [{'limit':501},{'limit':True},{'offset':-1},{'dataset':'unknown'}]:
   with self.assertRaises(ValueError):m.directory(**args)

if __name__=='__main__':unittest.main(verbosity=2)
