from pathlib import Path
from unittest.mock import Mock
import ast,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
LEGACY={'identity_ok':True,'signals':['OIL_BACKWARDATION: tight supply'],'product_data':{'CL':{'return_20d_pct':9}}}
class Tests(unittest.TestCase):
    def test_actual_snapshotter_cannot_copy_legacy_futures_features(self):
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-prediction-snapshotter/source/lambda_function.py').read_text(encoding='utf-8'))
        node=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='futures' for t in n.targets))
        scope={'_read_json':lambda key:LEGACY};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual snapshotter boundary','exec'),scope)
        self.assertFalse(scope['futures']['identity_ok']);self.assertEqual(scope['futures']['signals'],[])
        self.assertEqual(scope['futures']['product_data'],{});self.assertFalse(scope['futures']['calls_eligible'])
    def test_actual_alert_checker_cannot_emit_or_mark_legacy_curve_signals(self):
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-prepump-alerts-router/source/lambda_function.py').read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='check_futures_curves')
        marker=Mock(side_effect=AssertionError('No notification state change'));scope={'List':list,'_read_json':lambda key:LEGACY,'_is_new':marker,'_mark_alerted':marker}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual futures alert boundary','exec'),scope)
        self.assertEqual(scope['check_futures_curves']({}),[]);marker.assert_not_called()
if __name__=='__main__':unittest.main(verbosity=2)
