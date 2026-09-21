from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import Mock
from typing import List
import ast,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
from test_inflection_authority import functions
from test_global_liquidity_authority import block
LEGACY={'generated_at':'2026-09-21T12:00:00Z','regime':'NEGATIVE_GAMMA','total_gex':-9e9,
    'market_composite':{'composite_regime':'NEGATIVE_GAMMA'},'squeeze_candidates':[{'symbol':'SPY','score':100}],
    'underlyings':{'SPY':{'regime':'NEGATIVE_GAMMA','zero_gamma_flip_level':999,'call_walls_top5':[{'strike':990}],'put_walls_top5':[{'strike':980}]}}}


class Tests(unittest.TestCase):
    def test_actual_alert_checker_cannot_turn_legacy_regime_into_notification(self):
        forbidden=Mock(side_effect=AssertionError('No alert'))
        scope=functions('prepump-alerts-router',{'check_dealer_gex'},{'List':List,'_read_json':lambda key:LEGACY,'_is_new':forbidden,'_mark_alerted':forbidden})
        state={'last_gex_regime':'POSITIVE_GAMMA'};self.assertEqual(scope['check_dealer_gex'](state),[]);forbidden.assert_not_called()
    def test_actual_composite_does_not_count_legacy_source_presence_as_gamma_signal(self):
        scope=block('massive-signals','    sources = {}\n\n','    pof, pof_lm =',{'sources':{},'tickers':{},'_read':lambda key:(LEGACY,None)})
        self.assertIsNone(scope['gamma_regime']);self.assertEqual(scope['tickers'],{});self.assertFalse(scope['sources']['dealer_gex']['ok'])
        self.assertIsNone(scope['sources']['dealer_gex']['as_of'])
    def test_actual_best_setups_does_not_fall_back_to_root_or_invent_levels(self):
        text=(ROOT/'aws/lambdas/justhodl-best-setups/source/lambda_function.py').read_text(encoding='utf-8')
        start=text.index('        _og = __import__("option_population_context")');end=text.index('        _playbook_ctx =',start)
        import textwrap
        scope={'read_json':lambda key,*a:LEGACY};exec(compile(textwrap.dedent(text[start:end]),'actual best-setups boundary','exec'),scope)
        self.assertEqual(scope['_og'],{});self.assertEqual(scope['_walls'],{})
    def test_actual_confluence_gate_rejects_even_a_self_claimed_alpha_proof(self):
        path=ROOT/'aws/lambdas/justhodl-options-confluence/source/lambda_function.py';tree=ast.parse(path.read_text(encoding='utf-8'))
        fn=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='gated')
        scope={'TRUST_KEY':{'dealer-gex':'dealer_gex'},'trust_by':{'dealer_gex':{'alpha_status':'ALPHA_PROVEN','effective_trust':99}}}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),str(path),'exec'),scope)
        self.assertEqual(scope['gated']('dealer-gex'),(False,1.0))
    def test_actual_regime_module_abstains_without_neutral_vote(self):
        s3=Mock();s3.get_object.return_value={'Body':io.BytesIO(json.dumps(LEGACY).encode()),'LastModified':datetime.now(timezone.utc)}
        scope=functions('regime-composite',{'fetch_module'},{'S3':s3,'BUCKET':'synthetic','CISS_SERIES':{},'datetime':datetime,'timezone':timezone,'json':json})
        out=scope['fetch_module']({'key':'data/dealer-gex.json','label':'Dealer Gamma','emoji':'','dimension':'vol'})
        self.assertIsNone(out['polarity']);self.assertFalse(out['vote_eligible']);self.assertFalse(out['descriptive_eligible'])


if __name__=='__main__':unittest.main(verbosity=2)
