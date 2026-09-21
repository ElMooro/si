from pathlib import Path
import copy,sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'aws/shared'))
import option_population_context as m


class Tests(unittest.TestCase):
    def native(self):
        return {'contract':'dealer-gex-compatibility.v1',**{k:False for k in m.FLAGS},'underlyings':{},'market_composite':{},
            'source_capture_completed_at':'2026-09-21T12:00:00Z','generated_at':'2026-09-21T12:30:00Z',
            'canonical':{'key':'data/option-population-research.json','replay':{'manifest_key':'data/option-population-research/runs/'+'a'*64+'.json','output_sha256':'b'*64}}}
    def test_legacy_scores_and_boolean_authorities_cannot_restore_signals(self):
        for p in ({'underlyings':{'SPY':{'regime':'STRONG_NEGATIVE_GAMMA','total_dealer_gex_billions':-999}},'score':1,'calls_eligible':True},None,[],{}, {'canonical':['unexpected']}):
            out=m.context(p);self.assertEqual(out['underlyings'],{});self.assertEqual(out['independent_investment_votes'],0)
            self.assertFalse(out['calls_eligible']);self.assertIsNone(out['score']);self.assertFalse(out['native_reference_available'])
    def test_native_reference_preserves_source_clock_but_no_dealer_claims(self):
        p=self.native();out=m.context(p);self.assertTrue(out['native_reference_available'])
        self.assertEqual(out['generated_at'],p['source_capture_completed_at']);self.assertEqual(out['canonical'],p['canonical'])
        out['canonical']['replay']['output_sha256']='changed';self.assertNotEqual(out['canonical'],p['canonical'])
    def test_contract_identity_and_paths_required(self):
        for mutate in (lambda p:p.update(calls_eligible=True),lambda p:p['canonical'].update(key='data/trade-tickets.json'),
                lambda p:p.update(underlyings={'SPY':{'regime':'BULLISH'}}),lambda p:p['canonical'].update(replay='unexpected')):
            p=self.native();mutate(p);self.assertFalse(m.context(p)['native_reference_available'])
    def test_guard_only_changes_dealer_source(self):
        p={'unrelated':'whole'};self.assertIs(m.guard('data/other.json',p),p)
        self.assertEqual(m.guard('data/dealer-gex.json',p)['status'],'unqualified_legacy_or_unavailable')


if __name__=='__main__':unittest.main(verbosity=2)
