from pathlib import Path
from copy import deepcopy
import hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import capital_structure_context as gate
from test_capital_structure_research import fixture,compile_fixture


def packet():
    value=compile_fixture(fixture())['packet']
    value['replay']={'manifest_key':gate.PREFIX+'runs/'+'a'*64+'.json',
        'output_sha256':hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()}
    return value


class Tests(unittest.TestCase):
    def test_legacy_flags_scores_issuer_shards_and_original_fields_never_enter_decision_view(self):
        for value in (None,[],{}, {'call':'LONG','tickers':{'ABC':{'buyback_net_yield_pct':99,'flags':['INSIDER_CONVICTION']}}},packet()):
            out=gate.decision_view(value)
            self.assertEqual(out['tickers'],{});self.assertEqual(out['rows'],[])
            self.assertEqual(out['independent_investment_votes'],0)
            self.assertFalse(out['calls_eligible']);self.assertFalse(out['research_context']['dilution_qualified'])
            self.assertFalse(out['research_context']['buyback_signal_qualified'])
            self.assertEqual(out['research_context']['portfolio_action'],'WAIT')
        ctx=gate.context(packet());self.assertTrue(ctx['native_reference_available'])
        self.assertFalse(ctx['original_source_replay_performed_by_consumer'])
        self.assertFalse(ctx['current_freshness_verified_by_consumer'])

    def test_tamper_future_date_wrong_contract_and_unsafe_replay_paths_withhold_reference(self):
        original=packet()
        for change in (lambda p:p.update(reported_names=99),lambda p:p.update(generated_at='2999-01-01T00:00:00Z'),
                       lambda p:p.update(contract='legacy'),lambda p:p['replay'].update(manifest_key='data/portfolio.json'),
                       lambda p:p.update(calls_eligible=True),lambda p:p['quality'].update(split_basis_verified=True)):
            value=deepcopy(original);change(value)
            self.assertFalse(gate.context(value)['native_reference_available'])
            self.assertIsNone(gate.context(value)['canonical'])
        unrelated={'score':5};self.assertIs(gate.guard('unrelated.json',unrelated),unrelated)


if __name__=='__main__':unittest.main(verbosity=2)
