import unittest
from unittest.mock import Mock,patch
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
import oecd_series as source

SID='oecd:'+source.FLOW+':USA.M.PRVM.IX.BTE.Y._Z._Z.N'
class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('oecd_chart_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated data or provider')))
  return m
 def test_all_reviewed_keys_are_reachable_without_index_rebuild(self):
  m=self.module();self.assertEqual(m.browse('oecd:DSD_STES@DF_INDSERV')['total'],1511);self.assertEqual(m.browse('oecd:'+source.FLOW)['total'],1511)
  ids=[]
  for offset in range(0,1511,500):ids.extend(r['id'] for r in m.explorer({'provider':'oecd','kind':'series','limit':'500','offset':str(offset)})['rows'])
  self.assertEqual(len(set(ids)),1511);self.assertEqual(m.search(SID)['rows'][0]['id'],SID);self.assertEqual(m.search('United States',prov='oecd',kind='series')['rows'][0]['provider'],'oecd');m.load_index.assert_not_called()
 def test_native_resolution_preserves_version_and_never_uses_market_provider(self):
  m=self.module()
  for sid in (SID,SID+':G1',SID+':GY'):
   with patch.object(source,'fetch',return_value={'id':sid,'obs':[['2026-01-01',3]],'n':1}) as fetch:got=m.fetch_series(sid.upper())
   self.assertEqual(got['id'],sid);fetch.assert_called_once_with(sid,m.s3,m.BUCKET)
  m._tv_pull.assert_not_called();m._doc_lookup.assert_not_called();m.closest_ids.assert_not_called()
 def test_invalid_keys_stop_without_unknown_provider_fallback(self):
  m=self.module()
  for sid in ('oecd:unreviewed',SID.replace('USA','ZZZ'),SID.replace('4.3','4.2'),SID+':INVENTED'):
   with self.assertRaises(ValueError):m.fetch_series(sid)
  m._tv_pull.assert_not_called();m.closest_ids.assert_not_called()
 def test_source_unavailable_is_not_a_substitution(self):
  m=self.module()
  with patch.object(source,'fetch',return_value={'id':SID,'obs':[],'n':0,'quality':{'status':'unavailable'}}):got=m.fetch_series(SID)
  self.assertEqual(got['n'],0);m._cache_put.assert_not_called();m._tv_pull.assert_not_called()
 def test_other_dataset_rows_are_retained(self):
  m=self.module()
  for sid in ('oecd:DSD_STES@DF_INDSERV','oecd:ORIGINAL_OTHER_FLOW'):
   row=m.doc_row(m.doc(sid,'oecd','Original metadata','dataset',.3));self.assertEqual(row['id'],sid);self.assertFalse(row['chartable']);self.assertEqual(row['browse'],sid.endswith('DF_INDSERV'))
if __name__=='__main__':unittest.main()
