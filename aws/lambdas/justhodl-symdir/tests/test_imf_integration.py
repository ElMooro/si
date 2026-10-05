import io,json,unittest
from unittest.mock import Mock,patch
from datetime import datetime,timezone
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
import imf_series as source

SID='imf:LS:USA.U.PT.M'
class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('imf_chart_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated source')))
  return m
 def test_exact_flows_and_all_directory_rows_without_index(self):
  m=self.module();ids=[]
  for slug,cfg in source.CATALOGUE['flows'].items():
   self.assertEqual(m.browse('imf:'+slug)['total'],cfg['series']);self.assertEqual(m.browse('imf:'+slug.lower())['total'],cfg['series'])
  for offset in range(0,10113,500):ids += [r['id'] for r in m.explorer({'provider':'imf','kind':'series','offset':str(offset),'limit':'500'})['rows']]
  self.assertEqual(len(set(ids)),10113);self.assertEqual(m.search(SID)['rows'][0]['id'],SID);m.load_index.assert_not_called()
 def test_every_flow_canonicalizes_without_another_provider(self):
  m=self.module()
  for slug in source.CATALOGUE['flows']:
   sid='imf:'+next(k for k,v in source.CATALOGUE['series'].items() if v['flow']==slug)
   with patch.object(source,'fetch',return_value={'id':sid,'n':1,'obs':[['2026-01-01',3]]}) as fetch:
    self.assertEqual(m.fetch_series(sid.upper())['id'],sid);fetch.assert_called_once_with(sid,m.s3,m.BUCKET)
  m._doc_lookup.assert_not_called();m._tv_pull.assert_not_called();m.closest_ids.assert_not_called()
 def test_unknown_series_or_unreviewed_scaling_never_fall_back(self):
  m=self.module()
  for sid in [SID+':extra','imf:UNKNOWN:USA.U.PT.M','imf:LS:USA.UP.PE.M',SID.replace('USA','XXX')]:
   with self.assertRaises(ValueError):m.fetch_series(sid)
  m.closest_ids.assert_not_called();m.load_index.assert_not_called()
 def test_legacy_cache_cannot_bypass_definition_and_receipt(self):
  m=load_lambda('imf_chart_cache',SYMDIR,FakeS3())
  m.s3.get_object=Mock(return_value={'Body':io.BytesIO(json.dumps({'id':SID,'n':1,'obs':[['2026-01-01',999]]}).encode()),'LastModified':datetime.now(timezone.utc)})
  self.assertIsNone(m._cache_get(SID,86400)[0])
 def test_other_datasets_retained_and_only_reviewed_flow_overlaid(self):
  m=self.module();old=m.doc('imf:OTHER','imf','Other original dataset','dataset',.5);stale=m.doc('imf:LS','imf','Old metadata','dataset',.5)
  m.load_index=Mock(return_value={'docs':[old,stale]});m.hub=Mock(return_value={})
  page=m.explorer({'provider':'imf','limit':'100'});ids=[r['id'] for r in page['rows']]
  self.assertEqual(ids.count('imf:LS'),1);self.assertIn('imf:OTHER',ids);self.assertEqual(len(ids),5)
  self.assertEqual(next(r for r in page['rows'] if r['id']=='imf:LS')['reviewed_series'],2978)
 def test_legacy_rows_get_exact_source_units_and_periodicity(self):
  m=self.module();row=m.doc_row(m.doc(SID,'imf','Legacy label','series',.3,freq='D'));d=source.definition(SID)
  self.assertEqual(row['freq'],'M');self.assertEqual(row['unit'],'Percent of labor force');self.assertEqual(row['name'],d['name'])
if __name__=='__main__':unittest.main()
