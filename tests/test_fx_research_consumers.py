from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
import ast,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import fx_research_context as gate
LEGACY={'regime_signals':['USD_STRENGTHENING_20D (+9%)'],'regime_metrics':{'usd_synthetic_20d_pct':9},'fx_roro':{'fx_roro_score':99,'drivers':[{'driver':'carry'}]},'pair_data':{'USD_JPY':{'return_20d_pct':9}}}
def native():return {'contract':'fx-original-quote-research.v1','generated_at':'2020-01-01T00:00:00Z',
    'replay':{'manifest_key':'data/fx-quote-research/runs/'+'a'*64+'.json','output_sha256':'b'*64},**dict.fromkeys(gate.FLAGS,False),**LEGACY}


class Tests(unittest.TestCase):
    def test_neither_old_nor_self_promoted_native_packet_can_supply_signals(self):
        for p in (None,LEGACY,native(),{**native(),'calls_eligible':True}):
            out=gate.context(p)
            for name in ('pair_data','regime_metrics','fx_roro'):self.assertEqual(out[name],{})
            self.assertEqual(out['regime_signals'],[]);self.assertEqual(out['independent_investment_votes'],0)
            self.assertTrue(all(out[k] is False for k in gate.FLAGS))
    def test_reference_is_descriptive_and_does_not_claim_replay(self):
        out=gate.context(native());self.assertTrue(out['native_reference_available'])
        self.assertFalse(out['reference_hashes_independently_checked_by_consumer'])
        source=native();out=gate.context(source);out['canonical']['replay']['output_sha256']='changed'
        self.assertEqual(source['replay']['output_sha256'],'b'*64)
    def test_bad_clock_or_private_reference_cannot_be_native(self):
        for value in (None,'2020-01-01','9999-01-01T00:00:00Z'):
            self.assertFalse(gate.context({**native(),'generated_at':value})['native_reference_available'])
        p=native();p['replay']['manifest_key']='data/trade-tickets.json'
        self.assertFalse(gate.context(p)['native_reference_available'])
    def test_real_read_expressions_cannot_copy_legacy_fx_features(self):
        for function in ('prediction-snapshotter',):
            tree=ast.parse((ROOT/f'aws/lambdas/justhodl-{function}/source/lambda_function.py').read_text(encoding='utf-8'))
            node=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='fx' for t in n.targets))
            scope={'rd':lambda key:LEGACY,'_read_json':lambda key:LEGACY}
            exec(compile(ast.Module(body=[node],type_ignores=[]),function,'exec'),scope)
            self.assertEqual(scope['fx']['regime_signals'],[]);self.assertEqual(scope['fx']['fx_roro'],{})
        import flow_state_model
        read=Mock()
        with self.assertRaises(ValueError):flow_state_model.bind_parent('data/fx-quote-research.json',LEGACY,read,'2026-09-24T23:00:00Z')
        read.assert_not_called()
    def test_actual_fx_alert_checker_does_not_emit_or_mark_old_scores(self):
        path=ROOT/'aws/lambdas/justhodl-prepump-alerts-router/source/lambda_function.py';tree=ast.parse(path.read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='check_fx_regime')
        marker=Mock(side_effect=AssertionError('No FX notification state change permitted'));scope={'List':list,'_read_json':lambda key:LEGACY,'_is_new':marker,'_mark_alerted':marker}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual FX alert checker','exec'),scope)
        self.assertEqual(scope['check_fx_regime']({}),[]);marker.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
