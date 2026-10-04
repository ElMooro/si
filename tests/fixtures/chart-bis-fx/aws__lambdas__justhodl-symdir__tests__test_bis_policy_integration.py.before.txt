import unittest
from unittest.mock import Mock,patch
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
import bis_policy_series as b

class BISIntegrationTests(unittest.TestCase):
 def module(self):
  m=load_lambda('bis_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated storage or substitute')))
  return m
 def test_canonical_adapter_precedes_market_and_index(self):
  m=self.module();p={'id':'bis:WS_CBPOL:M.US','n':1,'obs':[['2025-01-01',0]]}
  with patch.object(b,'fetch',return_value=p) as f:r=m.fetch_series('BIS:WS_CBPOL:M.US')
  f.assert_called_once_with('bis:WS_CBPOL:M.US');self.assertEqual(r['id'],p['id']);m._doc_lookup.assert_not_called()
 def test_invalid_identifier_cannot_substitute_other_series(self):
  m=self.module()
  for sid in ['bis:WS_CBPOL:M.XX','bis:WS_CREDIT_GAP:Q.US']:
   with self.assertRaises(ValueError):m.fetch_series(sid)
  m.closest_ids.assert_not_called();m._tv_pull.assert_not_called()
 def test_provider_failure_cannot_fuzzy_fallback(self):
  m=self.module()
  with patch.object(b,'fetch',side_effect=ValueError('Provider denied')):
   with self.assertRaisesRegex(ValueError,'Provider denied'):m.fetch_series('bis:WS_CBPOL:D.US')
  m.closest_ids.assert_not_called();m._cache_put.assert_not_called()
 def test_reviewed_dataset_directory_and_exact_search_no_build(self):
  m=self.module();self.assertEqual(m.browse('bis:WS_CBPOL',limit=500)['total'],98);self.assertEqual(m.browse('bis:WS_CBPOL')['ds'],'bis:WS_CBPOL')
  self.assertEqual(m.search('bis:WS_CBPOL:M.US')['rows'][0]['id'],'bis:WS_CBPOL:M.US')
  self.assertEqual(m.explorer({'provider':'bis','kind':'series','limit':'500'})['total'],98)
  m.load_index.assert_not_called()

if __name__=='__main__':unittest.main()
