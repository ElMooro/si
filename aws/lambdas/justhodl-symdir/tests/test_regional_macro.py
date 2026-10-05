"""Original public macro histories plus calendar, units and failure regressions."""
from pathlib import Path
from datetime import datetime,timezone
import base64,copy,gzip,hashlib,io,json,math,re,unittest,urllib.error,zipfile
import regional_fed as m
import regional_macro_parser as parser
from test_regional_fed import Store,NOW
F=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-regional-macro/data'
SOURCES=json.loads((F/'sources.json').read_bytes());EXPECTED=json.loads(gzip.decompress((F/'independent-expected.json.gz').read_bytes()))
load=lambda n:gzip.decompress((F/(n+'.gz')).read_bytes())
WEI='regionalfed:dallas-wei:current';TRIM='regionalfed:cleveland-inflation-yoy:trimmedmeancpi'
class Reader:
 def __init__(self):self.calls=[];self.refusal=None
 def __call__(self,url):
  self.calls.append(url)
  if self.refusal:raise urllib.error.HTTPError(url,self.refusal,'Public source refused',{},None)
  row=next(r for r in SOURCES.values() if r['url']==url);return load(row['source_file']),{'content-type':'application/octet-stream'}
def extract(p):return json.loads(gzip.decompress(base64.b64decode(p['source_extract']['body_base64'])))
class Tests(unittest.TestCase):
 def test_all_14_original_histories_match_independent_readers_with_four_source_requests(self):
  self.assertEqual(len(EXPECTED),14);store=Store();reader=Reader()
  for sid,wanted in EXPECTED.items():
   with self.subTest(id=sid):
    p=m.fetch(sid,store,'invented',reader,NOW);self.assertIsNone(p['quality']['error']);self.assertEqual(p['obs'],wanted);self.assertEqual(p['n'],sum(v is not None for _,v in wanted));self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertFalse(p['equivalence_to_watchlist_provider_verified']);self.assertTrue(m.cache_valid(p,sid));self.assertFalse(p['history']['point_in_time_vintages_verified'])
    self.assertEqual(p['freq'],m.definition(sid)['freq']);self.assertEqual(len(extract(p)),len(wanted));self.assertEqual(m.retained_blob(store.objects[p['source_receipts'][0]['retained_key']],p['source_receipts'][0]),load(SOURCES[p['definition']['dataset']]['source_file']))
    for a,b in zip(p['obs'],wanted):
     if b[1] is not None and b[1]==0:self.assertEqual(math.copysign(1,a[1]),math.copysign(1,b[1]))
  self.assertEqual(len(reader.calls),4);self.assertEqual(len(set(reader.calls)),4)
 def test_weekly_dates_are_saturdays_not_month_starts_and_vintages_are_distinct(self):
  store=Store();reader=Reader();p=m.fetch(WEI,store,'invented',reader,NOW)
  self.assertEqual(p['freq'],'W');self.assertEqual(p['history']['period_precision'],'week');self.assertEqual(p['obs'][0],['2008-01-05',1.97]);self.assertEqual(p['obs'][-1],['2026-09-26',2.93]);self.assertTrue(all(datetime.fromisoformat(row[0]).weekday()==5 for row in p['obs']));self.assertEqual(p['measurement_evidence']['columns'][1],'original_reference_week')
  old=m.fetch('regionalfed:dallas-wei:vintage-2020-07-28',store,'invented',reader,NOW);self.assertEqual(old['obs'][0],['2008-01-05',1.41]);self.assertIsNone(old['obs'][-1][1]);self.assertIn('not independently',old['definition']['vintage'].replace('independently verified publication clock unavailable','not independently verified'));self.assertFalse(old['history']['point_in_time_vintages_verified']);self.assertIsNone(old['source_published_at']);self.assertEqual(len(reader.calls),1)
 def test_future_week_in_same_month_is_withheld(self):
  p=m.fetch(WEI,Store(),'invented',Reader(),datetime(2026,9,25,tzinfo=timezone.utc));self.assertIsNone(p['quality']['error']);self.assertEqual(p['obs'][-1],['2026-09-26',None]);self.assertEqual(extract(p)[-1]['rejection'],'future_reference_week_at_receipt');self.assertEqual(p['obs'][-2],['2026-09-19',3.02])
 def test_source_vintage_cannot_predate_its_own_label(self):
  p=m.fetch('regionalfed:dallas-wei:vintage-2025-09-25',Store(),'invented',Reader(),datetime(2025,9,24,tzinfo=timezone.utc));self.assertIsNone(p['quality']['error']);self.assertEqual(p['n'],0);self.assertTrue(all(r['rejection']=='publisher_vintage_after_receipt' for r in extract(p)));self.assertTrue(all(v is None for _,v in p['obs']))
 def test_monthly_fraction_annualized_rate_and_published_yoy_never_substitute(self):
  reader=Reader();store=Store();fraction=m.fetch('regionalfed:cleveland-trim-monthly:fraction',store,'invented',reader,NOW);annual=m.fetch('regionalfed:cleveland-trim-monthly:annualized',store,'invented',reader,NOW);yoy=m.fetch(TRIM,store,'invented',reader,NOW)
  self.assertEqual(fraction['obs'][-1],['2026-08-01',.002199451135668]);self.assertEqual(annual['obs'][-1],['2026-08-01',2.7]);self.assertEqual(yoy['obs'][-1],['2026-08-01',2.5698]);self.assertEqual(fraction['unit'],'fractional monthly change');self.assertEqual(annual['unit'],'percent annualized monthly change');self.assertEqual(yoy['unit'],'percent year-over-year change');self.assertEqual(len(reader.calls),2)
  self.assertEqual(yoy['definition']['reuse']['license'],'CC BY-SA 4.0');self.assertEqual(yoy['definition']['reuse']['attribution'],'Federal Reserve Bank of Cleveland')
 def test_publisher_interpolation_flags_follow_monthly_and_12_month_windows(self):
  monthly=m.fetch('regionalfed:cleveland-trim-monthly:fraction',Store(),'invented',Reader(),NOW);yoy=m.fetch(TRIM,Store(),'invented',Reader(),NOW)
  self.assertEqual([r['period'] for r in extract(monthly) if r['source_flags']],['2025-10','2025-11']);self.assertEqual([r['period'] for r in extract(yoy) if r['source_flags']],[f'2025-{i:02}' for i in (10,11,12)]+[f'2026-{i:02}' for i in range(1,9)])
  schema=m.CATALOGUE['datasets']['cleveland-inflation-yoy']['table_schema'];raw=b'date,mediancpi,trimmedmeancpi,cpi,corecpi\n2026-10-01,1,2,3,4\n2026-11-01,1,2,3,4\n';r=parser.parse(raw,schema)['mediancpi'];self.assertTrue(r[0]['source_flags']);self.assertFalse(r[1]['source_flags'])
 def test_refusal_is_stopped_without_weekly_or_monthly_fallback(self):
  reader=Reader();reader.refusal=403;store=Store()
  for sid in (WEI,'regionalfed:dallas-wei:vintage-2020-07-28'):
   p=m.fetch(sid,store,'invented',reader,NOW);self.assertEqual(p['obs'],[]);self.assertEqual(p['freq'],'W');self.assertFalse(p['history']['response_complete'])
  self.assertEqual(len(reader.calls),1)
 def test_cleveland_schema_order_duplicate_periods_and_wrong_day_are_rejected(self):
  ds=m.CATALOGUE['datasets']['cleveland-inflation-yoy'];raw=load(SOURCES['cleveland-inflation-yoy']['source_file']);lines=raw.decode().splitlines()
  for changed in [raw.replace(b'trimmedmeancpi',b'unknown',1),raw.replace(b'2017-01-01',b'2017-01-02',1),('\n'.join(lines+[lines[1]])).encode(),('\n'.join([lines[0],lines[2],lines[1]])).encode()]:
   with self.assertRaises(ValueError):parser.parse(changed,ds['table_schema'])
 def test_cleveland_missing_nonfinite_underflow_and_negative_zero_are_not_filled(self):
  ds=m.CATALOGUE['datasets']['cleveland-inflation-yoy'];prefix='date,mediancpi,trimmedmeancpi,cpi,corecpi\n2017-01-01,'
  for value in ('','NaN','true','1e999','1e-999'):
   row=parser.parse((prefix+value+',2,3,4\n').encode(),ds['table_schema'])['mediancpi'][0];self.assertIsNone(row['value']);self.assertEqual(row['original_value'],value)
  row=parser.parse((prefix+'-0,2,3,4\n').encode(),ds['table_schema'])['mediancpi'][0];self.assertEqual(math.copysign(1,row['value']),-1)
 def test_wei_headers_dates_extra_columns_and_formula_caches_are_checked(self):
  raw=load(SOURCES['dallas-wei']['source_file']);schema=m.CATALOGUE['datasets']['dallas-wei']['table_schema']
  def changed(before,after):
   stream=io.BytesIO();count=0
   with zipfile.ZipFile(io.BytesIO(raw)) as old,zipfile.ZipFile(stream,'w',zipfile.ZIP_DEFLATED) as new:
    for name in old.namelist():
     b=old.read(name);n=b.count(before);count+=n;b=b.replace(before,after) if n else b;new.writestr(name,b)
   self.assertEqual(count,1);return stream.getvalue()
  for bad in [changed(b'01/05/2008',b'01/06/2008'),changed(b'WEI as of 9/25/2025',b'WEI as of 9/25/2035')]:
   with self.assertRaises(ValueError):parser.parse(bad,schema)
  stream=io.BytesIO();count=0
  with zipfile.ZipFile(io.BytesIO(raw)) as old,zipfile.ZipFile(stream,'w',zipfile.ZIP_DEFLATED) as new:
   for name in old.namelist():
    body=old.read(name)
    if name=='xl/worksheets/sheet3.xml':body,count=re.subn(rb'(<c r="B2"[^>]*>).*?</c>',rb'\1<f>WEBSERVICE("https://never-fetch.invalid")</f><v>42</v></c>',body,count=1)
    new.writestr(name,body)
  self.assertEqual(count,1);row=parser.parse(stream.getvalue(),schema)['WEI'][0];self.assertEqual(row['value'],42);self.assertTrue(row['cached_formula_value'])
 def test_all_previous_644_definitions_and_11_datasets_are_unchanged(self):
  transition=json.loads((F.parent/'transition.json').read_bytes());row=transition['changes']['config/regional-fed-series.json'];old=json.loads((F.parents[3]/row['before_path']).read_bytes());self.assertEqual(len(old['series']),644)
  for key,value in old['series'].items():self.assertEqual(m.CATALOGUE['series'][key],value)
  for key,value in old['datasets'].items():self.assertEqual(m.CATALOGUE['datasets'][key],value)
  rows=m.directory(limit=500)['rows']+m.directory(limit=500,offset=500)['rows'];self.assertEqual(len(rows),658)
  for row in rows:self.assertEqual(row['freq'],m.definition(row['id'])['freq'])
  with self.assertRaises(ValueError):m.definition('regionalfed:cleveland-inflation-yoy:cpi')
if __name__=='__main__':unittest.main(verbosity=2)
