from pathlib import Path
from datetime import datetime,timezone
import base64,csv,gzip,hashlib,io,json,unittest,zipfile
from unittest.mock import patch
import census_series as m
from test_census_series import Store

R=Path(__file__).resolve().parents[4]
F=R/'tests/fixtures/chart-census-additional/data'
MANIFEST=json.loads((F/'manifest.json').read_bytes())
NOW=datetime(2026,10,5,12,tzinfo=timezone.utc)

def archive(slug):
 stream=io.BytesIO()
 with zipfile.ZipFile(stream,'w',zipfile.ZIP_DEFLATED) as z:
  z.writestr(slug.upper()+'-mf.csv',(F/(slug+'.csv')).read_bytes());z.writestr('/README',(F/'README-source.txt').read_bytes())
 return stream.getvalue()

def first_record_value(slug,value):
 # Test writer reconstructs the source dialect; original retained fixtures stay intact.
 rows=list(csv.reader(io.StringIO((F/(slug+'.csv')).read_text(encoding='utf-8'),newline='')))
 at=rows.index(['DATA'])+2;row=next(r for r in rows[at:] if r);row[-1]=value
 out=io.StringIO(newline='');csv.writer(out,lineterminator='\n').writerows(rows);return out.getvalue().encode()

class Additional(unittest.TestCase):
 def setUp(self):m._memory_source.clear();m._memory_index.clear()
 def test_all_original_definitions_and_twins_preserved(self):
  old=json.loads((R/'tests/fixtures/chart-census-additional/census-series.before.json').read_bytes())
  self.assertEqual(len(m.CATALOGUE['series']),13858);self.assertEqual(len(m.CATALOGUE['dataset_definitions']),20)
  for key,value in old['series'].items():self.assertEqual(m.CATALOGUE['series'][key],value)
  for key,value in old['dataset_definitions'].items():self.assertEqual(m.CATALOGUE['dataset_definitions'][key],value)
  root=R/'aws/lambdas/justhodl-symdir';self.assertEqual((root/'source/census-series.json').read_bytes(),(root/'config/census-series.json').read_bytes())
  self.assertNotIn('mhs',m.CATALOGUE['dataset_definitions'])
 def test_every_added_dataset_has_original_extract_and_canonical_identity(self):
  store=Store();calls=[]
  def reader(url):
   slug=next(k for k,v in m.CATALOGUE['dataset_definitions'].items() if v['source_url']==url);calls.append(slug);return archive(slug),{}
  for slug,entry in MANIFEST.items():
   self.assertEqual(hashlib.sha256((F/(slug+'.csv')).read_bytes()).hexdigest(),entry['fixture_sha256'])
   for sid in entry['series']:
    with self.subTest(sid=sid):
     p=m.fetch(sid,store,'fixture',reader=reader,now=NOW);self.assertEqual(p['id'],sid);self.assertTrue(p['history']['response_complete']);self.assertEqual(len(p['obs']),3)
     extract=gzip.decompress(base64.b64decode(p['source_extract']['body_base64']));rows=list(csv.DictReader(io.StringIO(extract.decode())))
     self.assertEqual(list(rows[0]),entry['columns']);self.assertEqual(len(rows),3)
     self.assertEqual(p['source_extract']['sha256'],hashlib.sha256(extract).hexdigest())
     for row in rows:self.assertEqual('et_idx' in row,len(entry['columns'])==7)
     self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertFalse(p['history']['point_in_time_vintages_verified'])
     self.assertTrue(m.cache_valid(p,sid))
  self.assertEqual(sorted(calls),sorted(MANIFEST));self.assertEqual(len(calls),14)
 def test_six_and_seven_column_profiles_cannot_be_interchanged(self):
  for slug in MANIFEST:
   raw=(F/(slug+'.csv')).read_bytes();headers=m.source_headers(slug)['DATA'];wrong=headers.copy()
   if 'et_idx' in wrong:wrong.remove('et_idx')
   else:wrong.insert(3,'et_idx')
   source=raw.replace(','.join(headers).encode(),','.join(wrong).encode())
   with self.subTest(slug=slug),self.assertRaises(ValueError):m.parse(source,m.definition(MANIFEST[slug]['series'][0]))
 def test_only_reviewed_narrative_notes_accept_unescaped_quotes(self):
  for slug in ['bfs','mtis']:
   raw=(F/(slug+'.csv')).read_bytes();heading=b'NOTES\n'
   self.assertEqual(raw.count(heading),1)
   changed=raw.replace(heading,heading+b'"Quoted "narrative" note"\n')
   self.assertEqual(m.parse(changed,m.definition(MANIFEST[slug]['series'][0]))[0],m.parse(raw,m.definition(MANIFEST[slug]['series'][0]))[0])
   data=raw.index(b'DATA\n')+len(b'DATA\n');start=raw.index(b'\n',data)+1;end=raw.index(b'\n',start)
   line=raw[start:end];bad=line[:line.rfind(b',')+1]+b'"12"oops'
   with self.assertRaises(csv.Error):m.parse(raw[:start]+bad+raw[end:],m.definition(MANIFEST[slug]['series'][0]))
  raw=(F/'m3.csv').read_bytes().replace(b'NOTES\n',b'NOTES\n"Quoted "narrative" note"\n')
  with self.assertRaises(csv.Error):m.parse(raw,m.definition(MANIFEST['m3']['series'][0]))
 def test_source_specific_markers_never_become_zero(self):
  for slug,marker,reason in [('bfs','S','estimate_fails_publication_quality_standard'),('bfs','D','confidentiality_withheld'),('mhs2','Z','estimate_below_50_units'),('mhs2','D','confidentiality_withheld'),('hv','(z)','estimate_rounds_to_zero'),('qpr','(z)','estimate_rounds_to_zero')]:
   with self.subTest(slug=slug,marker=marker):
    obs,records,_,_=m.parse(first_record_value(slug,marker),m.definition(MANIFEST[slug]['series'][0]));self.assertIsNone(obs[0][1]);self.assertEqual(records[0]['rejection'],reason)
    obs,records,_,_=m.parse(first_record_value(slug,'0'),m.definition(MANIFEST[slug]['series'][0]));self.assertEqual(obs[0][1],0);self.assertIsNone(records[0]['rejection'])
 def test_bfs_duration_and_projected_cohorts_have_distinct_meanings(self):
  for sid in MANIFEST['bfs']['series']:
   d=m.definition(sid);self.assertIn('Application cohort month',d['reference_period_scope'])
   if ':BF_DUR' in sid:self.assertEqual(d['unit'],'Quarters');self.assertEqual(d['observation_kind'],'formation_delay');self.assertEqual(d['unit_from_dictionary'],'Units')
   if ':BF_PBF' in sid:self.assertEqual(d['observation_kind'],'projected_formations')
   if ':BF_SBF' in sid:self.assertEqual(d['observation_kind'],'published_actual_projected_splice');self.assertIn('boundary not verified',d['measurement_basis'])
 def test_manufactured_shipments_are_annual_rates_without_rescaling(self):
  d=m.definition('census:mhs2:SH:T:yes:US');self.assertEqual(d['unit'],'Thousands of units (seasonally adjusted annual rate)');self.assertTrue(d['annual_rate'])
  obs,rows,_,_=m.parse((F/'mhs2.csv').read_bytes(),d)
  for (_,value),row in zip(obs,rows):self.assertEqual(value,float(row['original']['val']))
 def test_definition_drift_and_unreviewed_profiles_fail_closed(self):
  raw=(F/'bfs.csv').read_bytes().replace(b'UNITS',b'BILLIONS')
  with self.assertRaisesRegex(ValueError,'changed'):m.parse(raw,m.definition(MANIFEST['bfs']['series'][0]))
  for override in [{'has_error_dictionary':'false'},{'data_columns':['val']},{'notes_csv_strict':'false'}]:
   cfg={**m.CATALOGUE['dataset_definitions']['bfs'],**override}
   with patch.dict(m.CATALOGUE['dataset_definitions'],bfs=cfg),self.assertRaises(ValueError):m.sections((F/'bfs.csv').read_bytes(),'bfs')
 def test_previous_catalogue_packets_cannot_pass_current_cache(self):
  sid=MANIFEST['bfs']['series'][0];d=m.definition(sid)
  packet={'contract':m.CONTRACT,'id':sid,'definition':d,'definition_sha256':'0'*64,'history':{'response_complete':True,'source_snapshot_stale':False},'quality':{'error':None}}
  self.assertFalse(m.cache_valid(packet,sid))
 def test_resident_tables_are_bounded_and_eviction_reuses_retained_source(self):
  store=Store();calls=[];packets={}
  def reader(url):
   slug=next(k for k,v in m.CATALOGUE['dataset_definitions'].items() if v['source_url']==url);calls.append(slug);return archive(slug),{}
  for slug in ['m3','bfs','qfr','m3']:
   sid=MANIFEST[slug]['series'][0];packet=m.fetch(sid,store,'fixture',reader=reader,now=NOW)
   self.assertTrue(packet['history']['response_complete']);self.assertLessEqual(len(m._memory_index),2);self.assertLessEqual(len(m._memory_source),2)
   if sid in packets:self.assertEqual(packet['obs'],packets[sid]['obs']);self.assertEqual(packet['source_extract'],packets[sid]['source_extract'])
   packets[sid]=packet
  self.assertEqual(calls,['m3','bfs','qfr']);self.assertEqual(set(m._memory_index),{'qfr','m3'})

if __name__=='__main__':unittest.main(verbosity=2)
