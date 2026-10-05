import unittest
from unittest.mock import Mock,patch
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
import oecd_series as source

class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('oecd_additional_chart_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated data or provider')))
  return m
 def test_every_new_flow_directory_and_exact_search_available_without_index_rebuild(self):
  m=self.module()
  for flow in source._CONTEXTS:
   count=len(source._context(flow).CATALOG['series']);self.assertEqual(m.browse('oecd:'+flow)['total'],count);self.assertEqual(m.browse('oecd:'+flow.split(',')[1])['total'],count)
   key=next(iter(source._context(flow).CATALOG['series']));sid='oecd:'+flow+':'+key;self.assertEqual(m.search(sid)['rows'][0]['id'],sid)
  ids=[]
  for offset in range(0,3849,500):ids.extend(r['id'] for r in m.explorer({'provider':'oecd','kind':'series','offset':str(offset),'limit':'500'})['rows'])
  self.assertEqual(len(set(ids)),3849);m.load_index.assert_not_called()
 def test_each_flow_resolves_to_its_own_provider_with_canonical_case(self):
  m=self.module()
  for flow in source._CONTEXTS:
   sid='oecd:'+flow+':'+next(iter(source._context(flow).CATALOG['series']))
   with patch.object(source,'fetch',return_value={'id':sid,'obs':[['2026-01-01',3]],'n':1}) as fetch:got=m.fetch_series(sid.upper())
   self.assertEqual(got['id'],sid);fetch.assert_called_once_with(sid,m.s3,m.BUCKET)
  m._tv_pull.assert_not_called();m._doc_lookup.assert_not_called();m.closest_ids.assert_not_called()
 def test_unknown_version_or_key_cannot_fall_back(self):
  m=self.module()
  for flow in source._CONTEXTS:
   for sid in ['oecd:'+flow+':UNKNOWN','oecd:'+flow[:-1]+'9:USA.M.UNKNOWN']:
    with self.assertRaises(ValueError):m.fetch_series(sid)
  m._tv_pull.assert_not_called();m.closest_ids.assert_not_called()
 def test_other_dataset_metadata_unchanged_while_exact_reviewed_datasets_browse(self):
  m=self.module()
  for flow in source._CONTEXTS:
   row=m.doc_row(m.doc('oecd:'+flow.split(',')[1],'oecd','Original metadata','dataset',.3));self.assertTrue(row['browse']);self.assertFalse(row['chartable']);self.assertEqual(row['reviewed_series'],len(source._context(flow).CATALOG['series']))
  row=m.doc_row(m.doc('oecd:ORIGINAL_OTHER_FLOW','oecd','Original metadata','dataset',.3));self.assertFalse(row['browse']);self.assertFalse(row['chartable']);self.assertEqual(row['name'],'Original metadata')
if __name__=='__main__':unittest.main()
