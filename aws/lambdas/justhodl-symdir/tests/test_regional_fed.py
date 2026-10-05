from pathlib import Path
from datetime import datetime,timedelta,timezone
import base64,copy,gzip,hashlib,io,json,unittest,urllib.error
import regional_fed as m
F=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-regional-fed/data'
load=lambda n:gzip.decompress((F/(n+'.gz')).read_bytes())
CSV=load('cfnaiDataSeriesCsvCsv.csv');XLSX=load('kc-monthly.xlsx');PAGE=load('kc-manufacturing.html');NOW=datetime(2026,10,5,5,tzinfo=timezone.utc)
CHICAGO='regionalfed:chicago-cfnai:CFNAI';KC='regionalfed:kc-manufacturing:month-sa:composite'
class StoreError(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Store:
 def __init__(self):self.objects={};self.writes=[];self.fail_retention=False;self.denied=False
 def get_object(self,Bucket,Key):
  if self.denied:raise StoreError('AccessDenied')
  if Key not in self.objects:raise StoreError('NoSuchKey')
  return {'Body':io.BytesIO(self.objects[Key])}
 def put_object(self,Bucket,Key,Body,**kw):
  if self.denied or self.fail_retention and '/responses/' in Key:raise StoreError('AccessDenied')
  if kw.get('IfNoneMatch')=='*' and Key in self.objects:raise StoreError('PreconditionFailed')
  self.objects[Key]=Body;self.writes.append((Key,kw))
class Reader:
 def __init__(self):self.urls=[];self.refuse=None;self.csv=CSV
 def __call__(self,url):
  self.urls.append(url)
  if self.refuse:raise urllib.error.HTTPError(url,self.refuse,'Refused',{},None)
  return (self.csv if 'chicagofed.org' in url else XLSX if url.endswith('.xlsx') else PAGE),{'Content-Type':'application/octet-stream'}
class Tests(unittest.TestCase):
 def test_exact_identifiers_case_and_directory_never_infer_variants(self):
  rows=m.directory(limit=500)['rows']+m.directory(limit=500,offset=500)['rows'];self.assertEqual(len(rows),658);self.assertEqual(len(m.directory('chicago-cfnai')['rows']),7)
  for row in rows:
   self.assertEqual(m.definition(row['id'].upper())['id'],row['id']);self.assertIsNone(row['n']);self.assertFalse(row['live_history_verified'])
  for sid in [CHICAGO+':extra','regionalfed:kc-manufacturing:composite','ECONOMICS:USCFNAI','regionalfed:chicago-cfnai:UNKNOWN']:
   with self.assertRaises(ValueError):m.fetch(sid,None,None)
 def test_complete_chicago_packet_has_originals_and_source_calendar(self):
  store=Store();reader=Reader();p=m.fetch(CHICAGO,store,'bucket',reader,NOW);self.assertEqual(p['n'],714);self.assertEqual(p['last'],'2026-08-01');self.assertEqual(p['obs'][0],['1967-03-01',-.35]);self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertIsNone(p['source_published_at'])
  r=p['source_receipts'][0];self.assertEqual(m.retained_blob(store.objects[r['retained_key']],r),CSV);self.assertEqual(hashlib.sha256(CSV).hexdigest(),r['sha256']);self.assertEqual(len(reader.urls),1);self.assertTrue(m.cache_valid(p,CHICAGO))
  original=json.loads(gzip.decompress(base64.b64decode(p['source_extract']['body_base64'])));self.assertEqual(len(original),714);self.assertEqual(original[0]['source_location'],{'row':2,'column':'CFNAI'})
 def test_shared_dataset_cache_serves_all_seven_without_reacquisition(self):
  store=Store();reader=Reader()
  for sid in [d['id'] for d in m.CATALOGUE['series'].values() if d['dataset']=='chicago-cfnai']:
   p=m.fetch(sid,store,'bucket',reader,NOW);self.assertIsNone(p['quality']['error']);self.assertEqual(p['quality']['received_rows'],714)
  self.assertEqual(len(reader.urls),1)
 def test_kansas_city_discovery_and_original_workbook_retained_once_for_all_70(self):
  store=Store();reader=Reader()
  for d in m.CATALOGUE['series'].values():
   if d['dataset']!='kc-manufacturing':continue
   p=m.fetch(d['id'],store,'bucket',reader,NOW);self.assertIsNone(p['quality']['error']);self.assertEqual(len(p['obs']),303);self.assertEqual(len(p['source_receipts']),2);self.assertEqual(p['definition']['comparison'],d['comparison']);self.assertEqual(p['definition']['adjustment'],d['adjustment'])
  self.assertEqual(len(reader.urls),2);self.assertEqual(m.retained_blob(store.objects[p['source_receipts'][0]['retained_key']],p['source_receipts'][0]),PAGE);self.assertEqual(m.retained_blob(store.objects[p['source_receipts'][1]['retained_key']],p['source_receipts'][1]),XLSX)
 def test_source_refusal_blocks_dataset_without_retry_or_proxy(self):
  store=Store();reader=Reader();reader.refuse=403
  for sid in [CHICAGO,'regionalfed:chicago-cfnai:P_I']:
   p=m.fetch(sid,store,'bucket',reader,NOW);self.assertEqual(p['obs'],[]);self.assertEqual(p['n'],0);self.assertEqual(p['quality']['status'],'unavailable');self.assertFalse(p['history']['response_complete'])
  self.assertEqual(len(reader.urls),1)
 def test_retention_failure_or_denied_store_does_not_fabricate_history(self):
  for option in ['fail_retention','denied']:
   store=Store();setattr(store,option,True);reader=Reader();p=m.fetch(CHICAGO,store,'bucket',reader,NOW);self.assertEqual(p['obs'],[]);self.assertIsNotNone(p['quality']['error']);self.assertNotIn(m.namespace('chicago-cfnai')+'current.json',store.objects)
   self.assertEqual(len(reader.urls),1 if option=='fail_retention' else 0)
 def test_invalid_csv_consumes_daily_claim_without_repeated_source_requests(self):
  store=Store();reader=Reader();reader.csv=b'<html>not a dataset</html>'
  for _ in range(2):self.assertEqual(m.fetch(CHICAGO,store,'bucket',reader,NOW)['n'],0)
  self.assertEqual(len(reader.urls),1)
 def test_tampered_retained_original_is_rejected_without_refresh(self):
  store=Store();reader=Reader();p=m.fetch(CHICAGO,store,'bucket',reader,NOW);key=p['source_receipts'][0]['retained_key'];wrapper=json.loads(store.objects[key]);wrapper['body_base64']=base64.b64encode(CSV.replace(b'-0.35',b'99999',1)).decode();store.objects[key]=json.dumps(wrapper).encode();p=m.fetch(CHICAGO,store,'bucket',reader,NOW);self.assertEqual(p['obs'],[]);self.assertEqual(len(reader.urls),1)
 def test_manifest_clock_cannot_refresh_old_originals(self):
  store=Store();reader=Reader();m.fetch(CHICAGO,store,'bucket',reader,NOW);key=m.namespace('chicago-cfnai')+'current.json';manifest=json.loads(store.objects[key]);manifest['acquired_at']=(NOW+timedelta(days=1)).isoformat();store.objects[key]=json.dumps(manifest).encode();p=m.fetch(CHICAGO,store,'bucket',reader,NOW+timedelta(days=1));self.assertEqual(p['obs'],[]);self.assertEqual(len(reader.urls),1)
 def test_corrupt_workbook_never_escapes_as_a_successful_partial_dataset(self):
  store=Store();calls=[]
  def reader(url):calls.append(url);return (b'not zip' if url.endswith('.xlsx') else PAGE),{}
  p=m.fetch(KC,store,'bucket',reader,NOW);self.assertEqual(p['obs'],[]);self.assertIn('ValueError',p['quality']['error']);self.assertEqual(len(calls),2)
 def test_future_reference_month_is_retained_but_withheld(self):
  store=Store();reader=Reader();reader.csv=b'Date,P_I,EU_H,C_H,SO_I,CFNAI,CFNAI_MA3,DIFFUSION\n2027/01,1,2,3,4,5,6,7\n';p=m.fetch(CHICAGO,store,'bucket',reader,NOW);self.assertEqual(p['obs'],[['2027-01-01',None]]);self.assertEqual(p['quality']['rejected_rows'],1);self.assertEqual(p['measurement_evidence']['rows'][0][4],'5');self.assertEqual(p['measurement_evidence']['rows'][0][5],'future_reference_month_at_receipt')
 def test_schema_and_source_receipt_tampering_never_qualify(self):
  store=Store();reader=Reader();blobs,receipts,clock=m.snapshot('chicago-cfnai',store,'bucket',reader,NOW)
  for key,val in [('url','https://evil.invalid/x'),('bytes',1),('dataset','kc-manufacturing'),('dataset_definition_sha256','0'*64),('retained_key','elsewhere')]:
   bad=copy.deepcopy(receipts);bad[0][key]=val
   with self.assertRaises(ValueError):m.packet(CHICAGO,blobs,bad,clock)
  for value in [{},dict(m.fetch(CHICAGO,store,'bucket',reader,NOW),definition_sha256='0'*64)]:self.assertFalse(m.cache_valid(value,CHICAGO))
if __name__=='__main__':unittest.main(verbosity=2)
