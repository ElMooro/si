from pathlib import Path
import io,json,unittest
from unittest.mock import Mock,patch
from datetime import datetime,timezone
from test_universal_provider_search import load_lambda,FakeS3
import defillama_tvl as source
SYMDIR=Path(__file__).resolve().parents[1]/'source/lambda_function.py';SID='defillama:tvl:all'
class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('defillama_chart_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated source')))
  return m
 def test_complete_directory_search_and_pagination_do_not_load_unrelated_index(self):
  m=self.module();rows=m.browse('defillama:reviewed-tvl-history',limit=500)['rows'];self.assertEqual(len(rows),469);self.assertEqual(len({r['id'] for r in rows}),469)
  self.assertEqual(m.explorer({'provider':'defillama','kind':'series','limit':'500'})['rows'],rows);self.assertEqual(m.search(SID)['rows'][0]['id'],SID)
  paged=[]
  for offset in range(0,469,50):paged+=m.browse('defillama:reviewed-tvl-history',limit=50,offset=offset)['rows']
  self.assertEqual(paged,rows);m.load_index.assert_not_called()
 def test_every_canonical_definition_avoids_market_or_token_substitution(self):
  m=self.module()
  for d in source.CATALOGUE['series'].values():
   sid=d['id']
   with patch.object(source,'fetch',return_value={'id':sid,'n':1,'obs':[['2026-01-01',3]]}) as fetch:
    self.assertEqual(m.fetch_series(sid.upper())['id'],sid);fetch.assert_called_once_with(sid,m.s3,m.BUCKET)
  m.closest_ids.assert_not_called();m._tv_pull.assert_not_called();m._doc_lookup.assert_not_called()
 def test_unknown_ids_never_fall_back_or_match_partial_chain_names(self):
  m=self.module()
  for sid in [SID+':extra','defillama:tvl:ETH','defillama:tvl:Unknown','defillama:TOTAL_TVL']:
   with self.assertRaises(ValueError):m.fetch_series(sid)
  m.closest_ids.assert_not_called();m.load_index.assert_not_called()
 def test_legacy_cache_has_no_authority(self):
  m=load_lambda('defillama_chart_cache',SYMDIR,FakeS3());m.s3.get_object=Mock(return_value={'Body':io.BytesIO(json.dumps({'id':SID,'n':1,'obs':[['2026-01-01',999]]}).encode()),'LastModified':datetime.now(timezone.utc)})
  self.assertIsNone(m._cache_get(SID,86400)[0])
 def test_other_datasets_preserved_and_reviewed_dataset_overlaid_once(self):
  m=self.module();m.load_index=Mock(return_value={'docs':[m.doc('defillama:OTHER','defillama','Existing dataset','dataset',.5),m.doc('defillama:reviewed-tvl-history','defillama','Old label','dataset',.5)]});m.hub=Mock(return_value={})
  rows=m.explorer({'provider':'defillama','limit':'100'})['rows'];self.assertEqual({r['id'] for r in rows},{'defillama:OTHER','defillama:reviewed-tvl-history'});self.assertEqual(len(rows),2);row=next(r for r in rows if r['id']=='defillama:reviewed-tvl-history');self.assertTrue(row['browse']);self.assertFalse(row['chartable']);self.assertEqual(row['reviewed_series'],469)
 def test_exact_metadata_is_USD_stock_not_borrowed_or_flow_measurement(self):
  m=self.module();row=m.doc_row(m.doc(SID,'defillama','Old metadata','series',.3,freq='M'));self.assertEqual(row['freq'],'D');self.assertEqual(row['unit'],'USD');self.assertEqual(row['provider'],'defillama');self.assertFalse(row['live_history_verified'])
if __name__=='__main__':unittest.main(verbosity=2)
