from pathlib import Path
from datetime import datetime,timedelta,timezone
import base64,copy,gzip,hashlib,io,json,sys,unittest,urllib.error,xml.etree.ElementTree as E
import imf_series as m
D=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-imf/data';NOW=datetime(2026,10,5,3,tzinfo=timezone.utc)
IDS={'LS':'imf:LS:USA.U.PT.M','PI':'imf:PI:USA.IND.YOY_PCH_PT.M','PPI':'imf:PPI:USA.PPI.YOY_PCH_PT.M','MFS_IR':'imf:MFS_IR:USA.MFS166_RT_PT_A_PT.M'}
RAW={k:(D/(k+'.xml')).read_bytes() for k in IDS};local=lambda e:e.tag.rsplit('}',1)[-1]
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
def mutate(fn,flow='LS'):
 root=E.fromstring(RAW[flow]);dataset=next(e for e in root if local(e)=='DataSet');series=next(e for e in dataset if local(e)=='Series');fn(root,dataset,series);return E.tostring(root,encoding='utf-8')
class Tests(unittest.TestCase):
 def setUp(self):self.store=Store();self.calls=[]
 def reader(self,url):
  self.calls.append(url);flow=next(k for k in IDS if m.definition(IDS[k])['source_url']==url);return RAW[flow],{'Content-Type':'application/xml'}
 def fetch(self,flow='LS',**kw):return m.fetch(IDS[flow],self.store,'fixture',reader=kw.pop('reader',self.reader),now=kw.pop('now',NOW),**kw)
 def test_exact_catalogue_definitions(self):
  rows=[]
  for offset in range(0,10113,500):rows.extend(m.directory(limit=500,offset=offset)['rows'])
  self.assertEqual(len(rows),10113);self.assertEqual(len({r['id'] for r in rows}),10113)
  for row in rows:self.assertEqual(m.definition(row['id'].upper())['id'],row['id']);self.assertFalse(row['live_history_verified'])
  self.assertEqual(m.directory(flow='LS',limit=1)['total'],2978)
 def test_exact_identity_no_guesses_or_currency_scaling(self):
  for sid in ['imf:LS','imf:LS:USA.UP.PE.M',IDS['PI']+'.extra','imf:MFS_CBS:ARM.S121_L_MB_CBS.XDC.A','imf:PI:UNKNOWN.IND.IX.M','https://evil.test']:
   with self.assertRaises(ValueError):m.fetch(sid,self.store,'fixture',reader=self.reader,now=NOW)
  self.assertEqual(self.calls,[]);self.assertEqual(self.store.puts,[])
 def test_complete_source_and_observations_retained(self):
  for flow,sid in IDS.items():
   p=self.fetch(flow);self.assertGreater(p['n'],500);self.assertTrue(m.cache_valid(p,sid));self.assertFalse(p['calls_eligible']);self.assertFalse(p['history']['full_upstream_history_verified'])
   r=p['source_receipts'][0];self.assertEqual(self.store.objects[r['retained_key']],RAW[flow]);self.assertEqual(hashlib.sha256(RAW[flow]).hexdigest(),r['sha256'])
   extract=json.loads(gzip.decompress(base64.b64decode(p['source_extract']['body_base64'])));root=E.fromstring(RAW[flow]);s=next(e for e in root.iter() if local(e)=='Series');self.assertEqual(extract['observations'],[o.attrib for o in s]);self.assertEqual(extract['series_attributes'],s.attrib)
   q=self.fetch(flow);self.assertEqual(p['obs'],q['obs']);self.assertEqual(len(self.calls),list(IDS).index(flow)+1)
 def test_invalid_definition_version_or_access_is_not_retained(self):
  mutations=[lambda r,d,s:s.set('SCALE','6'),lambda r,d,s:s.set('COUNTRY','CAN'),lambda r,d,s:s.set('REFERENCE_PERIOD','2020=100'),lambda r,d,s:s.set('SECURITY_CLASSIFICATION','FOU'),lambda r,d,s:d.set('ACCESS_SHARING_LEVEL','ALL_IMF_STAFF'),lambda r,d,s:s[0].set('ACCESS_SHARING_LEVEL','ALL_IMF_STAFF'),lambda r,d,s:next(e for e in r.iter() if local(e)=='Ref').set('version','999'),lambda r,d,s:next(e for e in r.iter() if local(e)=='Test').__setattr__('text','true'),lambda r,d,s:d.append(copy.deepcopy(s)),lambda r,d,s:r.append(E.Element('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message/footer}Footer'))]
  for fn in mutations:
   self.store=Store();p=self.fetch(reader=lambda _: (mutate(fn),{}));self.assertEqual(p['n'],0);self.assertTrue(p['quality']['error']);self.assertFalse(any('/responses/' in k for k in self.store.puts))
 def test_missing_forecasts_status_and_duplicate_periods(self):
  def change(r,d,s):
   s[0].set('OBS_VALUE','0');s[1].set('STATUS','F');s[2].set('STATUS','K');s[3].set('OBS_VALUE','NaN');s[4].set('STATUS','B');s[5].set('STATUS','new');s[6].set('DERIVATION_TYPE','new');s[7].set('TIME_PERIOD','2000-M13');s[8].set('TIME_PERIOD',s[9].attrib['TIME_PERIOD'])
  p=self.fetch(reader=lambda _: (mutate(change),{}));rows=p['measurement_evidence']['rows'];self.assertEqual(rows[0][3],0)
  for pos in (1,2,3,5,6,7,8,9):self.assertIsNone(rows[pos][3]);self.assertIsNotNone(rows[pos][7])
  self.assertIsNotNone(rows[4][3]);self.assertEqual(rows[4][5],'B');self.assertEqual(p['quality']['source_status_counts']['B'],1)
 def test_shared_gate_and_http_refusal_are_not_retried(self):
  count=[]
  def denied(url):count.append(url);raise urllib.error.HTTPError(url,403,'Forbidden',{},None)
  p=self.fetch(reader=denied);q=self.fetch(reader=denied);other=self.fetch('PI',reader=denied)
  self.assertEqual(len(count),1);self.assertTrue(p['quality']['error']);self.assertIn('stopped',q['quality']['error']);self.assertIn('stopped',other['quality']['error']);self.assertFalse(any('/responses/' in k for k in self.store.puts))
 def test_cache_denial_and_failed_retention_do_not_return_data(self):
  self.store.deny=True;p=self.fetch();self.assertEqual(self.calls,[]);self.assertEqual(p['n'],0)
  self.store=Store();self.store.fail_retention=True;p=self.fetch();self.assertEqual(p['n'],0);self.assertEqual(len(self.calls),1)
 def test_corrupt_retained_data_does_not_trigger_provider_fallback(self):
  p=self.fetch();key=p['source_receipts'][0]['retained_key'];self.store.objects[key]+=b'corrupt';q=self.fetch();self.assertEqual(q['n'],0);self.assertEqual(len(self.calls),1)
 def test_one_daily_request_and_future_clocks(self):
  p=self.fetch();self.assertGreater(p['n'],0)
  q=self.fetch(now=NOW-timedelta(seconds=1));self.assertEqual(q['n'],0);self.assertEqual(len(self.calls),1)
  d=m.definition(IDS['LS']);self.store.objects[m.prefix(d)+'current.json']=b'{}';q=self.fetch();self.assertEqual(q['n'],0);self.assertEqual(len(self.calls),1)
 def test_calendar_anchors_are_not_daily_or_annual_substitutions(self):
  self.assertEqual(m.period('2026-Q3','Q'),'2026-07-01');self.assertEqual(m.period('2026','A'),'2026-01-01');self.assertIsNone(m.period('2026-Q5','Q'));self.assertIsNone(m.period('2026-M03','A'));self.assertIsNone(m.period('2026-03-05','M'))
 def test_source_precision_is_retained_and_plot_rounding_disclosed(self):
  def change(r,d,s):s[0].set('OBS_VALUE','1.234567890123456789');s[1].set('OBS_VALUE','1e-9999');s[2].set('OBS_VALUE','1e9999')
  p=self.fetch(reader=lambda _: (mutate(change),{}));rows=p['measurement_evidence']['rows'];self.assertEqual(rows[0][4],'1.234567890123456789');self.assertEqual(rows[0][3],float(rows[0][4]));self.assertTrue(rows[0][8]);self.assertIsNone(rows[1][3]);self.assertIsNone(rows[2][3]);self.assertGreaterEqual(p['quality']['plot_rounding_rows'],1)
 def test_group_units_and_missing_metadata_fail_closed(self):
  def change_unit(r,d,s):next(e for e in d if local(e)=='Group' and 'UNIT' in e.attrib).set('UNIT','USD')
  def remove_group(r,d,s):d.remove(next(e for e in d if local(e)=='Group' and 'UNIT' in e.attrib))
  for change in (change_unit,remove_group):
   self.store=Store();p=self.fetch('PI',reader=lambda _: (mutate(change,'PI'),{}));self.assertEqual(p['n'],0);self.assertFalse(any('/responses/' in k for k in self.store.puts))
if __name__=='__main__':unittest.main(verbosity=2)
