import io,json,unittest
from unittest.mock import Mock,patch
from datetime import datetime,timezone
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
import cboe_index as source
SID='cboeindex:VIX6M'
class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('cboe_chart_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated source')))
  return m
 def test_complete_directory_and_search_without_loading_unrelated_index(self):
  m=self.module();rows=m.browse('cboe:reviewed-index-history',limit=500)['rows'];self.assertEqual(len(rows),77);self.assertEqual(len({r['id'] for r in rows}),77);self.assertEqual({r['provider'] for r in rows},{'cboeindex'})
  for provider in ['cboe','cboeindex']:
   self.assertEqual(m.explorer({'provider':provider,'kind':'series','limit':'500'})['rows'],rows);self.assertEqual(m.search('VIX6M',prov=provider,kind='series')['rows'][0]['id'],SID)
  self.assertEqual(m.search(SID)['rows'][0]['id'],SID);m.load_index.assert_not_called()
 def test_case_normalization_and_all_definitions_avoid_market_sources(self):
  m=self.module()
  for symbol in source.CATALOGUE['series']:
   sid='cboeindex:'+symbol
   with patch.object(source,'fetch',return_value={'id':sid,'n':1,'obs':[['2026-01-01',3]]}) as fetch:
    self.assertEqual(m.fetch_series(sid.upper())['id'],sid);fetch.assert_called_once_with(sid,m.s3,m.BUCKET)
  m.closest_ids.assert_not_called();m._tv_pull.assert_not_called();m._doc_lookup.assert_not_called()
 def test_unknown_ids_fail_without_fallback_or_partial_dimension_match(self):
  m=self.module()
  for sid in [SID+':extra','cboeindex:VX1!','cboeindex:UNKNOWN']:
   with self.assertRaises(ValueError):m.fetch_series(sid)
  m.closest_ids.assert_not_called();m.load_index.assert_not_called()
 def test_legacy_cache_has_no_authority(self):
  m=load_lambda('cboe_chart_cache',SYMDIR,FakeS3());m.s3.get_object=Mock(return_value={'Body':io.BytesIO(json.dumps({'id':SID,'n':1,'obs':[['2026-01-01',999]]}).encode()),'LastModified':datetime.now(timezone.utc)})
  self.assertIsNone(m._cache_get(SID,86400)[0])
 def test_other_datasets_preserved_and_reviewed_dataset_overlaid_once(self):
  m=self.module();m.load_index=Mock(return_value={'docs':[m.doc('cboe:OTHER','cboe','Existing dataset','dataset',.5),m.doc('cboe:reviewed-index-history','cboe','Old label','dataset',.5)]});m.hub=Mock(return_value={})
  rows=m.explorer({'provider':'cboe','limit':'100'})['rows'];self.assertEqual({r['id'] for r in rows},{'cboe:OTHER','cboe:reviewed-index-history'});self.assertEqual(len(rows),2);row=next(r for r in rows if r['id']=='cboe:reviewed-index-history');self.assertTrue(row['browse']);self.assertFalse(row['chartable']);self.assertEqual(row['reviewed_series'],77)
 def test_exact_series_metadata_uses_source_unit_and_scalar_namespace(self):
  m=self.module();row=m.doc_row(m.doc(SID,'cboe','Old metadata','series',.3,freq='M'));self.assertEqual(row['freq'],'D');self.assertEqual(row['unit'],'Volatility index points');self.assertEqual(row['provider'],'cboeindex')
if __name__=='__main__':unittest.main()
