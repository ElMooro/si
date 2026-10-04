import unittest
from unittest.mock import Mock,patch
from test_universal_provider_search import load_lambda,SYMDIR,FakeS3
from test_bis_fx_cross import packet
import bis_fx_series as source
import bis_fx_cross as cross

class Tests(unittest.TestCase):
 def module(self):
  m=load_lambda('bis_fx_integration',SYMDIR,FakeS3());m._cache_get=Mock(return_value=(None,None));m._cache_put=Mock()
  for name in ['_doc_lookup','closest_ids','load_index','_tv_pull']:setattr(m,name,Mock(side_effect=AssertionError('Unrelated storage or substitute')))
  return m
 def test_new_exact_dataset_and_original_policy_are_both_complete(self):
  m=self.module();self.assertEqual(m.browse('bis:WS_CBPOL')['total'],98);self.assertEqual(m.browse('bis:WS_XRU')['total'],1234)
  ids=[]
  for offset in range(0,1332,500):ids.extend(row['id'] for row in m.explorer({'provider':'bis','kind':'series','limit':'500','offset':str(offset)})['rows'])
  self.assertEqual(len(ids),1332);self.assertEqual(len(set(ids)),1332)
  self.assertEqual(sum(i.startswith('bis:WS_CBPOL:') for i in ids),98)
  self.assertEqual(m.search('bis:WS_XRU:D.JP.JPY.A')['rows'][0]['id'],'bis:WS_XRU:D.JP.JPY.A');m.load_index.assert_not_called()
 def test_canonical_exact_fx_uses_native_adapter_before_market(self):
  m=self.module();doc=packet('bis:WS_XRU:D.JP.JPY.A',[('2026-01-02',150)])
  with patch.object(source,'fetch',return_value=doc) as fetch:got=m.fetch_series('BIS:WS_XRU:D.JP.JPY.A')
  fetch.assert_called_once_with(doc['id']);self.assertEqual(got['obs'],doc['obs']);m._tv_pull.assert_not_called()
 def test_cross_joins_exact_sources_and_preserves_null(self):
  m=self.module();d=cross.definition('bisfx:XDR:JPY:D');rows={d['numerator']:packet(d['numerator'],[('2026-01-02',150),('2026-01-03',None)]),d['denominator']:packet(d['denominator'],[('2026-01-02',.75),('2026-01-03',.8)])}
  with patch.object(source,'fetch',side_effect=rows.__getitem__) as fetch:got=m.fetch_series('BISFX:XDR:JPY:D')
  self.assertEqual(got['obs'],[['2026-01-02',200],['2026-01-03',None]]);self.assertEqual(fetch.call_count,2)
  m._doc_lookup.assert_not_called();m.closest_ids.assert_not_called();m._tv_pull.assert_not_called()
 def test_failed_source_cannot_try_another_provider(self):
  m=self.module()
  with patch.object(source,'fetch',return_value={'n':0,'history':{'response_complete':False}}) as fetch:
   with self.assertRaises(ValueError):m.fetch_series('bisfx:XDR:JPY:D')
  self.assertEqual(fetch.call_count,1);m.closest_ids.assert_not_called();m._tv_pull.assert_not_called()
 def test_invalid_identity_never_fuzzy_resolves(self):
  m=self.module()
  for sid in ['bis:WS_XRU:D.JP.USD.A','bisfx:XDR:FAKE:D','bisfx:JPY:VND:D']:
   with self.assertRaises(ValueError):m.fetch_series(sid)
  m.closest_ids.assert_not_called();m._tv_pull.assert_not_called()

if __name__=='__main__':unittest.main()
