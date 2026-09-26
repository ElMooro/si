from pathlib import Path
from copy import deepcopy
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
import master_allocation_authority as policy
from master_allocation_publication import validate
from public_brain_projection import sanitize_public
START='2026-09-26T15:20:00Z';NOW='2026-09-26T15:25:00Z'


class Tests(unittest.TestCase):
    def setUp(self):
        self.packet=sanitize_public(policy.CURRENT,policy.project({'as_of':'2026-09-26T15:20:31Z',
            'target_allocation':{'stock':30,'cash':70},'confidence':30,'best_asset':{'ranked':[{'asset':'SPY'},{'asset':'CASH'}]}}))
        self.machine=policy.execution_packet(self.packet)

    def test_whole_projection_and_machine_target_match(self):
        out=validate(self.packet,self.machine,START,NOW)
        self.assertTrue(out['projection_replayed']);self.assertTrue(out['machine_target_is_null'])
        self.assertEqual(out['retained_weight_count'],2);self.assertEqual(out['retained_momentum_rows'],2)
        self.assertFalse(out['original_market_sources_replayed'])

    def test_legacy_older_future_naive_and_partial_heads_are_rejected(self):
        for change in ({'contract':'legacy'},{'generated_at':'2026-09-26T12:20:00Z'},
            {'generated_at':'2026-09-26T16:20:00Z'},{'generated_at':'2026-09-26T15:20:00'},
            {'unqualified_projection':None}):
            with self.assertRaises(ValueError):validate({**self.packet,**change},self.machine,START,NOW)

    def test_any_actionable_or_differently_typed_public_field_is_rejected(self):
        for change in ({'target_allocation':{}},{'calls_eligible':0},{'sizing_eligible':True},
                       {'execution_eligible':True},{'confidence':0},{'extra':True}):
            with self.assertRaisesRegex(ValueError,'typed public'):validate({**self.packet,**change},self.machine,START,NOW)

    def test_stale_actionable_or_mismatched_machine_target_is_rejected(self):
        for change in ({'target':{}},{'target':{'stock':100}},{'execution_eligible':0},
            {'as_of':'2026-09-26T12:20:00Z'},{'posture':'RISK_ON'},{'extra':True}):
            with self.assertRaisesRegex(ValueError,'Machine-readable'):validate(self.packet,{**self.machine,**change},START,NOW)

    def test_missing_calculation_clock_or_inventory_is_not_a_complete_projection(self):
        for change in ({'as_of':'2026-09-26T15:19:59Z'},{'target_allocation':None},{'best_asset':{}}):
            packet=deepcopy(self.packet);packet['unqualified_projection'].update(change)
            with self.assertRaises(ValueError):validate(packet,self.machine,START,NOW)


if __name__=='__main__':unittest.main(verbosity=2)
