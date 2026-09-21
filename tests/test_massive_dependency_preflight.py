from pathlib import Path
import hashlib,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6001_massive_dependency_preflight as audit
from test_options_dependency_preflight import Store


class Tests(unittest.TestCase):
    def test_whole_original_retains_unknown_fields_and_formatting(self):
        raw=b'{ "tickers": {"A": {"prepump_score":0}}, "future_field":[1,2,3] }\n'
        s=Store(raw);ref,info=audit.capture(s,'data/massive-signals.json')
        self.assertEqual(s.saved[ref['key']],raw);self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(info['ticker_count'],1);self.assertIn('future_field',info['root_fields'])
    def test_account_path_rejected_before_read(self):
        s=Store(b'{}')
        with self.assertRaises(AssertionError):audit.capture(s,'data/trade-tickets.json')
        self.assertEqual(s.reads,[])
    def test_corrupt_readback_fails(self):
        s=Store(b'{}');s.corrupt=True
        with self.assertRaises(AssertionError):audit.capture(s,'data/massive-signals.json')
    def test_publication_time_does_not_become_pair_observation(self):
        info=audit.describe('data/polygon-fx-regime.json',{'generated_at':'2026-09-21T12:00:00Z','pair_data':{'EUR_USD':{'latest_price':0},'USD_JPY':{'latest_price':None}}})
        self.assertEqual(info['pair_count'],2);self.assertTrue(all(n==0 for n in info['row_clocks'].values()))
        self.assertIsNone(info['authority']['calls_eligible'])
    def test_root_replay_reference_does_not_certify_replay(self):
        info=audit.describe('data/option-flow-research.json',{'replay':{'manifest_key':'untrusted'},'calls_eligible':False})
        self.assertFalse(info['replay_verified_by_this_inventory']);self.assertIs(info['authority']['calls_eligible'],False)
    def test_quarantine_and_zero_product_count_remain_explicit(self):
        info=audit.describe('data/polygon-futures-curves.json',{'status':'QUARANTINED','identity_ok':False,'n_products':8,'n_products_with_data':0,'product_data':{'S&P':[]}})
        self.assertIs(info['identity_ok'],False);self.assertEqual(info['products_with_data'],0);self.assertEqual(info['declared_products'],8)
    def test_wrong_root_shape_is_rejected(self):
        with self.assertRaises(AssertionError):audit.describe('data/massive-signals.json',[])


if __name__=='__main__':unittest.main()
