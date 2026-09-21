"""Independent rational checks must reject damaged coefficient results."""
from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import option_population_model as model
import test_option_flow_store as fixtures
from ops_5996_option_population_qualification import independent


class Tests(unittest.TestCase):
    def setUp(self):
        t=fixtures.StoreTests();t.setUp()
        try:s3,inputs,out,ref=t.candidate()
        finally:t.tearDown()
        self.read=model.evidence.reader(s3,'synthetic');self.source=inputs['chains']['SPY']
        self.output=model.chain({**out,'replay':ref},'SPY',self.read)
        self.summary=model.upstream.checked(out['chains']['SPY']['chain'],self.read,'chains')
    def verify(self):return independent(self.output,self.summary,self.source,self.read)
    def test_exact_source_rationals_match(self):
        result=self.verify();self.assertTrue(result['exact_rational_reconciliation']);self.assertEqual(result['returned_rows'],2)
    def test_wrong_total_fails(self):
        self.output['totals']['sides']['call']['gamma_oi_shares']['value']='1'
        with self.assertRaises(AssertionError):self.verify()
    def test_wrong_expiry_strike_group_fails(self):
        self.output['by_expiry_strike'][0]['sides']['call']['delta_oi_shares']['value']='100'
        with self.assertRaises(AssertionError):self.verify()
    def test_missingness_cannot_become_complete(self):
        self.output['totals']['sides']['call']['gamma_oi_shares']['included_rows']=100
        with self.assertRaises(AssertionError):self.verify()
    def test_zero_cannot_become_missing(self):
        self.output['totals']['sides']['call']['reported_open_interest']['value']=None
        with self.assertRaises((AssertionError,TypeError)):self.verify()
    def test_invented_observation_and_flip_rejected(self):
        for field in ('zero_gamma_flip','observed_dealer_inventory','dealer_hedging_flow'):
            original=self.output[field];self.output[field]=0
            with self.assertRaises(AssertionError):self.verify()
            self.output[field]=original
        self.output['totals']['sides']['call']['gamma_oi_shares']['observation_time']='2026-09-21'
        with self.assertRaises(AssertionError):self.verify()


if __name__=='__main__':unittest.main(verbosity=2)
