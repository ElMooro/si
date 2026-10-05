"""Complete retained public histories plus adversarial, network-free cases."""
from pathlib import Path
from datetime import datetime,timezone
import base64,copy,gzip,hashlib,io,json,math,re,unittest,urllib.error,zipfile
import regional_fed as m
import regional_survey_parser as parser
from test_regional_fed import Store,NOW
F=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-regional-surveys/data'
SOURCES=json.loads((F/'sources.json').read_bytes())
EXPECTED=json.loads(gzip.decompress((F/'independent-expected.json.gz').read_bytes()))
load=lambda n:gzip.decompress((F/(n+'.gz')).read_bytes())
class Reader:
 def __init__(self):self.calls=[];self.refusal=None
 def __call__(self,url):
  self.calls.append(url)
  if self.refusal:raise urllib.error.HTTPError(url,self.refusal,'Public source refused',{},None)
  row=next(r for r in SOURCES.values() if r['url']==url);return load(row['source_file']),{'content-type':'application/octet-stream'}
class Tests(unittest.TestCase):
 def test_all_567_original_histories_match_independent_replay_and_share_nine_downloads(self):
  self.assertEqual(len(EXPECTED),567);store=Store();reader=Reader()
  for sid,obs in EXPECTED.items():
   with self.subTest(id=sid):
    packet=m.fetch(sid,store,'invented-offline-bucket',reader,NOW)
    self.assertIsNone(packet['quality']['error']);self.assertEqual(packet['obs'],obs);self.assertEqual(packet['n'],sum(v is not None for _,v in obs))
    self.assertEqual(packet['quality']['received_rows'],len(obs));self.assertFalse(packet['calls_eligible']);self.assertFalse(packet['sizing_eligible']);self.assertFalse(packet['equivalence_to_watchlist_provider_verified']);self.assertTrue(m.cache_valid(packet,sid))
    receipt=packet['source_receipts'][0];source=SOURCES[packet['definition']['dataset']];self.assertEqual(receipt['sha256'],source['sha256']);self.assertEqual(m.retained_blob(store.objects[receipt['retained_key']],receipt),load(source['source_file']))
    records=json.loads(gzip.decompress(base64.b64decode(packet['source_extract']['body_base64'])));self.assertEqual(len(records),len(obs));self.assertTrue(all('original_reference_period' in r['source_location'] for r in records))
    for actual,wanted in zip(packet['obs'],obs):
     if wanted[1]==0 and wanted[1] is not None:self.assertEqual(math.copysign(1,actual[1]),math.copysign(1,wanted[1]))
  self.assertEqual(len(reader.calls),9);self.assertEqual(len(set(reader.calls)),9)
 def test_dictionary_conflicts_are_not_searchable_or_chartable(self):
  for key in ('sa_mfg_emp_e','nsa_mfg_emp_e'):
   sid='regionalfed:richmond-manufacturing:'+key
   self.assertNotIn(sid,m.CATALOGUE['series']);self.assertEqual(m.directory(q=sid)['rows'],[])
   with self.assertRaises(ValueError):m.definition(sid)
 def test_units_horizons_and_inventories_are_not_inferred_from_symbol_letters(self):
  definitions=m.CATALOGUE['series']
  self.assertEqual(definitions['regionalfed:ny-empire-sa:GACDSA']['unit'],'percent of respondents')
  self.assertEqual(definitions['regionalfed:ny-empire-sa:GACDISA']['unit'],'diffusion index points')
  self.assertEqual(definitions['regionalfed:philadelphia-mbos:cefdfsa']['expectation_horizon_months'],6)
  self.assertIsNone(definitions['regionalfed:dallas-manufacturing-sa:Fgi']['expectation_horizon_months'])
  self.assertIn('finished',definitions['regionalfed:dallas-manufacturing-sa:Fgi']['name'].lower())
  self.assertIn('October 2025',definitions['regionalfed:richmond-nonmanufacturing:sa_svc_revs_sales_c']['interpretation'])
  for d in definitions.values():
   if d['measurement_kind']=='survey_reported_price_change' and d['source_key'].endswith('_e'):self.assertIsNone(d['expectation_horizon_months']);self.assertIn('unverified',d['interpretation'])
 def test_old_seven_and_seventy_definitions_remain_byte_identical_as_objects(self):
  transition=json.loads((F.parent/'transition.json').read_bytes());row=transition['changes']['config/regional-fed-series.json'];root=F.parents[3];old=json.loads((root/row['before_path']).read_bytes())
  self.assertEqual(len(old['series']),77)
  for key,value in old['series'].items():self.assertEqual(m.CATALOGUE['series'][key],value)
  for key,value in old['datasets'].items():self.assertEqual(m.CATALOGUE['datasets'][key],value)
 def test_failure_stops_only_that_dataset_and_has_no_cross_source_fallback(self):
  store=Store();reader=Reader();reader.refusal=403
  for sid in ('regionalfed:ny-empire-sa:GACDISA','regionalfed:ny-empire-sa:NOCDISA'):
   packet=m.fetch(sid,store,'invented',reader,NOW);self.assertEqual(packet['obs'],[]);self.assertEqual(packet['quality']['status'],'unavailable')
  self.assertEqual(len(reader.calls),1);self.assertTrue(all('esms_seasonallyadjusted_allseries.csv' in url for url in reader.calls))
 def test_reference_calendar_uses_documented_century_and_month_not_publication_time(self):
  schema=m.CATALOGUE['datasets']['philadelphia-mbos']['table_schema'];self.assertEqual(parser.period('May-68',schema),('1968-05',None));self.assertEqual(parser.period('Sep-26',schema),('2026-09',None))
  schema=m.CATALOGUE['datasets']['dallas-manufacturing-nsa']['table_schema'];self.assertEqual(parser.period('April-14',schema),('2014-04',None))
  for bad in ('Apr-2004','Foo-24','Sep-99'):
   with self.assertRaises(ValueError):parser.period(bad,schema)
  schema=m.CATALOGUE['datasets']['ny-empire-sa']['table_schema']
  with self.assertRaises(ValueError):parser.period('2026-09-01',schema)
 def test_schema_shift_duplicate_or_unsorted_months_fail_instead_of_relabelling(self):
  dataset='ny-empire-sa';schema=m.CATALOGUE['datasets'][dataset]['table_schema'];raw=load(SOURCES[dataset]['source_file']);lines=raw.decode('utf-8-sig').splitlines()
  inputs=[raw.replace(b'GACDISA',b'UNKNOWN',1),('\n'.join(lines+[lines[1]])).encode(),('\n'.join([lines[0],lines[2],lines[1]])).encode(),('\n'.join([lines[0],lines[1]+',extra'])).encode()]
  for item in inputs:
   with self.assertRaises(ValueError):parser.parse(item,schema)
 def test_source_missing_bounds_negative_zero_and_nonfinite_are_explicit(self):
  field={'numeric_bounds':[-100,100],'outside_bounds_policy':'withhold'}
  make=lambda text,f=field:parser.observation(0,'2026-01',{'text':text,'kind':'numeric','formula':False},{'row':2},f,['ND','#N/A',''])
  for token in ('ND','#N/A',''):
   row=make(token);self.assertIsNone(row['value']);self.assertEqual(row['rejection'],'source_missing_value');self.assertEqual(row['original_value'],token)
  self.assertEqual(math.copysign(1,make('-0')['value']),-1)
  for token in ('1e999','1e-999','true','NaN','101'):
   self.assertIsNone(make(token)['value'])
  adjusted=make('101',dict(field,outside_bounds_policy='retain_flagged'));self.assertEqual(adjusted['value'],101);self.assertIn('published_adjusted_value_outside_unadjusted_bounds',adjusted['source_flags'])
 def test_workbook_nonnumeric_cells_and_formula_cache_are_not_executed(self):
  dataset='dallas-manufacturing-sa';ds=m.CATALOGUE['datasets'][dataset];parsed=parser.parse(load(SOURCES[dataset]['source_file']),ds['table_schema']);records=[r for values in parsed.values() for r in values]
  self.assertTrue(all(r['original_value'] is not None for r in records))
  out=io.BytesIO();changed=0
  with zipfile.ZipFile(io.BytesIO(load(SOURCES[dataset]['source_file']))) as old,zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED) as new:
   for name in old.namelist():
    body=old.read(name)
    if name=='xl/worksheets/sheet1.xml':body,changed=re.subn(rb'(<c r="B2"[^>]*>).*?</c>',rb'\1<f>WEBSERVICE("https://must-never-fetch.invalid")</f><v>42</v></c>',body,count=1)
    new.writestr(name,body)
  self.assertEqual(changed,1);rows=parser.parse(out.getvalue(),ds['table_schema'])[ds['table_schema']['columns'][1]];self.assertTrue(rows[0]['cached_formula_value']);self.assertEqual(rows[0]['value'],42);self.assertEqual(rows[0]['original_value'],'42')
  row=parser.observation(0,'2026-01',{'text':'42','kind':'s','formula':True},{'cell':'B2'},{'numeric_bounds':[-100,100]},[]);self.assertIsNone(row['value']);self.assertTrue(row['cached_formula_value'])
 def test_changed_workbook_headers_dates_and_unlabelled_cells_are_rejected(self):
  dataset='richmond-manufacturing';ds=m.CATALOGUE['datasets'][dataset];raw=load(SOURCES[dataset]['source_file'])
  def transform(before,after):
   out=io.BytesIO()
   with zipfile.ZipFile(io.BytesIO(raw)) as old,zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED) as new:
    found=False
    for name in old.namelist():
     body=old.read(name)
     if name=='xl/worksheets/sheet1.xml':self.assertIn(before,body);body=body.replace(before,after,1);found=True
     new.writestr(name,body)
   self.assertTrue(found);return out.getvalue()
  for modified in [transform(b'r="B2"',b'r="ZZ2"'),transform(b'r="A2"',b'r="A3"')]:
   with self.assertRaises(ValueError):parser.parse(modified,ds['table_schema'])
 def test_future_reference_months_never_become_observed_values(self):
  ds='ny-empire-sa';store=Store();reader=Reader();p=m.fetch('regionalfed:'+ds+':GACDISA',store,'invented',reader,datetime(2026,8,1,tzinfo=timezone.utc));self.assertIsNone(p['quality']['error']);self.assertIsNone(p['obs'][-1][1]);self.assertEqual(p['measurement_evidence']['rows'][-1][5],'future_reference_month_at_receipt')
if __name__=='__main__':unittest.main(verbosity=2)
