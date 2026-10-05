from pathlib import Path
import io,json,unittest
from datetime import datetime,timezone
from unittest.mock import Mock,patch
from test_universal_provider_search import load_lambda,FakeS3
import regional_fed as source
SYMDIR=Path(__file__).resolve().parents[1]/'source/lambda_function.py';SID='regionalfed:chicago-cfnai:CFNAI'
class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('regional_fed_chart_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated source')))
  return m
 def test_exact_two_dataset_browsing_and_full_directory_need_no_unrelated_index(self):
  m=self.module();a=m.browse('regionalfed:chicago-cfnai',limit=500)['rows'];b=m.browse('regionalfed:kc-manufacturing',limit=500)['rows'];self.assertEqual(len(a),7);self.assertEqual(len(b),70)
  rows=m.explorer({'provider':'regionalfed','kind':'series','limit':'500'})['rows'];self.assertEqual(len(rows),77);self.assertEqual({r['id'] for r in rows},{r['id'] for r in a+b});self.assertEqual(m.search(SID)['rows'][0]['id'],SID)
  self.assertEqual(m.browse('regionalfed:kc-manufacturing',limit=50)['rows']+m.browse('regionalfed:kc-manufacturing',offset=50,limit=50)['rows'],b);m.load_index.assert_not_called()
 def test_all_canonical_definitions_keep_case_comparison_adjustment_and_exact_source(self):
  m=self.module()
  for d in source.CATALOGUE['series'].values():
   sid=d['id']
   with patch.object(source,'fetch',return_value={'id':sid,'n':1,'obs':[['2026-01-01',3]]}) as fetch:
    self.assertEqual(m.fetch_series(sid.upper())['id'],sid);fetch.assert_called_once_with(sid,m.s3,m.BUCKET)
  m.closest_ids.assert_not_called();m._tv_pull.assert_not_called();m._doc_lookup.assert_not_called()
 def test_unknown_ids_never_use_other_providers_or_infer_an_adjustment(self):
  m=self.module()
  for sid in [SID+':extra','regionalfed:kc-manufacturing:composite','regionalfed:chicago-cfnai:UNKNOWN','regionalfed:UNKNOWN']:
   with self.assertRaises(ValueError):m.fetch_series(sid)
  with self.assertRaises(ValueError):m.browse('regionalfed:UNKNOWN')
  m.closest_ids.assert_not_called();m.load_index.assert_not_called()
 def test_legacy_cache_cannot_supply_unqualified_observations(self):
  m=load_lambda('regional_fed_cache',SYMDIR,FakeS3());m.s3.get_object=Mock(return_value={'Body':io.BytesIO(json.dumps({'id':SID,'n':1,'obs':[['2026-01-01',999]]}).encode()),'LastModified':datetime.now(timezone.utc)})
  self.assertIsNone(m._cache_get(SID,86400)[0])
 def test_two_reviewed_datasets_overlay_without_deleting_existing_provider_entries(self):
  m=self.module();m.load_index=Mock(return_value={'docs':[m.doc('regionalfed:OTHER','regionalfed','Existing dataset','dataset',.5),m.doc('regionalfed:chicago-cfnai','regionalfed','Old label','dataset',.5)]});m.hub=Mock(return_value={})
  rows=m.explorer({'provider':'regionalfed','limit':'100'})['rows'];self.assertEqual({r['id'] for r in rows},{'regionalfed:OTHER','regionalfed:chicago-cfnai','regionalfed:kc-manufacturing'});self.assertEqual(len(rows),3)
  for row in rows:
   if row['id']=='regionalfed:OTHER':continue
   self.assertTrue(row['browse']);self.assertFalse(row['chartable']);self.assertEqual(row['reviewed_series'],7 if row['id'].endswith('chicago-cfnai') else 70)
 def test_doc_metadata_uses_source_units_and_monthly_frequency(self):
  m=self.module();row=m.doc_row(m.doc(SID,'regionalfed','Old metadata','series',.3,freq='D'));self.assertEqual(row['freq'],'M');self.assertEqual(row['unit'],'CFNAI standard-deviation units');self.assertEqual(row['provider'],'regionalfed');self.assertIsNone(row['currency']);self.assertFalse(row['live_history_verified'])
if __name__=='__main__':unittest.main(verbosity=2)
