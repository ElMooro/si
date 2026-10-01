"""Exercise the actual publication boundary without engine side effects."""
from pathlib import Path
from copy import deepcopy
import ast,sys,unittest
SOURCE=Path(__file__).resolve().parents[1]/'source';sys.path.insert(0,str(SOURCE))
from bond_vol_boundary import apply_bond_vol_boundary
class Tests(unittest.TestCase):
    def fixture(self):return {'dump_risk_score':85,'risk_level':'EXTREME','action':'Sell','top_drivers':[{'factor':'macro'}],
        'factors':{'macro_regime':{'weight':.06,'risk':50,'note':'benign'},'observed':{'risk':10,'raw':0}}}
    def test_no_imputation_no_reweighting_no_portfolio_permission(self):
        original=self.fixture();out=apply_bond_vol_boundary(original,{'calls_eligible':False,'replay':{'manifest_key':'proof'}})
        self.assertEqual(original,self.fixture());self.assertIsNone(out['dump_risk_score']);self.assertIsNone(out['factors']['macro_regime']['risk'])
        self.assertEqual(out['factors']['observed'],original['factors']['observed']);self.assertEqual(out['top_drivers'],[])
        self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible']);self.assertFalse(out['execution_eligible']);self.assertEqual(out['decision']['verb'],'WAIT')
    def test_legacy_path_unchanged(self):
        out=self.fixture();self.assertIs(apply_bond_vol_boundary(out,{}),out)
    def test_actual_handler_publishes_guarded_values_and_cannot_trip_high_risk_alert(self):
        tree=ast.parse((SOURCE/'lambda_function.py').read_text(encoding='utf-8'));handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        start=next(i for i,n in enumerate(handler.body) if isinstance(n,ast.ImportFrom) and n.module=='bond_vol_boundary')
        statements=handler.body[start:start+3];self.assertIsInstance(handler.body[start+3],ast.Expr)
        self.assertIn('s3.put_object',ast.unparse(handler.body[start+3]));scope={'out':self.fixture(),'bond':{'calls_eligible':False}}
        exec(compile(ast.Module(body=statements,type_ignores=[]),'actual-handler-boundary','exec'),scope)
        self.assertIsNone(scope['composite']);self.assertEqual(scope['level'],'UNAVAILABLE');self.assertEqual(scope['drivers'],[])
        tripwire=next(n for n in ast.walk(handler) if isinstance(n,ast.If) and 'prev_level' in ast.unparse(n.test))
        scope['prev_level']='LOW';self.assertFalse(eval(compile(ast.Expression(tripwire.test),'actual-tripwire','eval'),scope))
if __name__=='__main__':unittest.main()
