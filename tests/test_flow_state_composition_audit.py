from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'tests')]
from flow_state_composition_audit import independent
from test_flow_state_model import fixture
import flow_state_model as model

class Tests(unittest.TestCase):
    def test_source_subtotals_units_and_coverage_reconcile(self):
        inputs,blobs,docs=fixture();out=model.compile_output(inputs,blobs.__getitem__)
        counts=independent(out,docs)
        self.assertEqual(counts['positive_estimate_categories'],3);self.assertEqual(counts['negative_estimate_categories'],0)
        self.assertEqual(counts['overlapping_funds'],2);self.assertEqual(counts['monthly_transaction_windows'],2)
        self.assertFalse(counts['provider_original_replay_performed_by_this_audit'])
    def test_altered_sign_units_overlap_and_legacy_repromotion_fail(self):
        inputs,blobs,docs=fixture();out=model.compile_output(inputs,blobs.__getitem__)
        for change in (lambda p:p['asset_class_rotation'][1].update(direction='net_redemption_estimate'),
            lambda p:p['monthly_transactions'][0].update(unit='usd'),
            lambda p:p.update(category_overlap=[]),lambda p:p['dark_pool'].update(accumulation_n=99)):
            bad=copy.deepcopy(out);change(bad)
            with self.assertRaises(AssertionError):independent(bad,docs)
if __name__=='__main__':unittest.main(verbosity=2)
