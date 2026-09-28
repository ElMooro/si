"""Synthetic isolated Apex input block; no handler/learning log/notification I/O."""
from pathlib import Path
import ast,json,unittest
ROOT=Path(__file__).resolve().parents[2]
SRC=ROOT/'aws/lambdas/justhodl-apex-fusion/source/lambda_function.py'


class Tests(unittest.TestCase):
    def test_legacy_and_forged_eligibility_cannot_supply_positioning_scores(self):
        tree=ast.parse(SRC.read_bytes());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_positioning_candidates_for_scoring')
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loop=next(n for n in handler.body if isinstance(n,ast.For) and isinstance(n.iter,ast.Call) and isinstance(n.iter.func,ast.Name) and n.iter.func.id==fn.name)
        for packet in (None,{}, {'candidates':[{'ticker':'SYNA','directional_score':100}]},
                       {'ranking_eligible':True,'forecast_qualified':True,'candidates':[{'ticker':'SYNA','convergence_score':100}]},
                       {'measurement_contract':'positioning-price-observations.v1','price_observations':[{'ticker':'SYNA'}]}):
            calls=[];scope={'pp':packet,'slot':lambda *a:calls.append(a)}
            exec(compile(ast.Module(body=[fn,loop],type_ignores=[]),'<isolated Apex input only>','exec'),scope)
            self.assertEqual(calls,[])
    def test_original_input_and_exclusion_reason_remain_declared(self):
        text=SRC.read_text(encoding='utf-8')
        self.assertIn('pp = _rd("data/pump-positioning.json")',text)
        self.assertIn('"reason": "positioning_forecast_unqualified"',text)
        original=(ROOT/'tests/fixtures/pre-momentum-leaders-apex-fusion.py.txt').read_bytes()
        self.assertGreater(len(original),10000);self.assertIn(b'for c in (pp.get("candidates") or []):',original)
    def test_other_repaired_consumers_never_read_legacy_position_sizes(self):
        # Their pure outputs already abstain independently of this new input.
        for name,path in [('brief','aws/lambdas/justhodl-pump-radar-brief/source/brief_evidence.py'),
                          ('summary','aws/lambdas/justhodl-prepump-summary/source/summary_evidence.py')]:
            tree=ast.parse((ROOT/path).read_bytes())
            literals={n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str)}
            self.assertNotIn('position_size_pct',literals,name);self.assertIn('abstain',literals,name)


if __name__=='__main__':unittest.main(verbosity=2)
