import io,json,unittest
from unittest.mock import Mock,patch
from datetime import datetime,timezone
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
import census_series as source

SID='census:marts:MPCSM:44X72:yes:US'
class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('census_chart_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated source')))
  return m
 def test_all_six_exact_dataset_pages_and_exact_search_without_index(self):
  m=self.module();ids=[]
  for slug,cfg in source.CATALOGUE['dataset_definitions'].items():
   self.assertEqual(m.browse('census:'+slug)['total'],cfg['series']);self.assertEqual(m.browse('census:'+slug.upper())['total'],cfg['series'])
  for offset in range(0,3402,500):ids += [r['id'] for r in m.explorer({'provider':'census','kind':'series','offset':str(offset),'limit':'500'})['rows']]
  self.assertEqual(len(set(ids)),3402);self.assertEqual(m.search(SID)['rows'][0]['id'],SID);m.load_index.assert_not_called()
 def test_each_dataset_canonicalizes_and_never_looks_up_another_provider(self):
  m=self.module()
  for slug in source.CATALOGUE['dataset_definitions']:
   sid='census:'+next(k for k,v in source.CATALOGUE['series'].items() if v['dataset']==slug)
   with patch.object(source,'fetch',return_value={'id':sid,'n':1,'obs':[['2026-01-01',3]]}) as fetch:
    self.assertEqual(m.fetch_series(sid.upper())['id'],sid);fetch.assert_called_once_with(sid,m.s3,m.BUCKET)
  m._doc_lookup.assert_not_called();m._tv_pull.assert_not_called();m.closest_ids.assert_not_called()
 def test_unknown_dimensions_and_extra_fields_do_not_fall_back(self):
  m=self.module()
  for sid in [SID+':extra',SID.replace('MPCSM','UNKNOWN'),SID.replace(':yes:',':guess:')]:
   with self.assertRaises(ValueError):m.fetch_series(sid)
  m.closest_ids.assert_not_called();m.load_index.assert_not_called()
 def test_legacy_cache_cannot_bypass_reviewed_definition(self):
  m=load_lambda('census_chart_cache',SYMDIR,FakeS3())
  m.s3.get_object=Mock(return_value={'Body':io.BytesIO(json.dumps({'id':SID,'n':1,'obs':[['2026-01-01',999]]}).encode()),'LastModified':datetime.now(timezone.utc)})
  self.assertIsNone(m._cache_get(SID,86400)[0])
 def test_existing_census_dataset_and_other_provider_metadata_retained(self):
  m=self.module();old=m.doc('census:other','census','Other original dataset','dataset',.5)
  stale=m.doc('census:mrts','census','Old metadata','dataset',.5);old_series=m.doc('census:other:old:no:US','census','Other original series','series',.4)
  m.load_index=Mock(return_value={'docs':[old,stale,old_series]});m.hub=Mock(return_value={})
  page=m.explorer({'provider':'census','limit':'100'});ids=[r['id'] for r in page['rows']]
  self.assertEqual(ids.count('census:mrts'),1);self.assertIn('census:other',ids);self.assertIn('census:other:old:no:US',ids)
  self.assertEqual(next(r for r in page['rows'] if r['id']=='census:mrts')['reviewed_series'],568)
 def test_reviewed_legacy_directory_rows_get_exact_units_and_frequency(self):
  m=self.module();key=next(k for k,d in source.CATALOGUE['series'].items() if d['dataset']=='qss');sid='census:'+key
  row=m.doc_row(m.doc(sid,'census','Legacy label','series',.3,freq='M'));d=source.definition(sid)
  self.assertEqual(row['freq'],'Q');self.assertEqual(row['unit'],d['unit']);self.assertEqual(row['name'],d['name'])

if __name__=='__main__':unittest.main()
