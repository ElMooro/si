"""Only isolated candidate loops with synthetic data; no downstream native I/O."""
from pathlib import Path
import ast,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'tests/ops'))
from test_cascade_evidence import capture,AT
from cascade_evidence import build


def tree(name):return ast.parse((ROOT/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py').read_bytes())
def forbidden(*a,**kw):raise AssertionError('No live data, learning, notifications, trading or calibration')
def execute(nodes,ns):exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated synthetic candidate loops>','exec'),ns)


class Tests(unittest.TestCase):
    def packet(self):return build(*capture(),AT)
    def test_trade_ticket_candidate_loop_gets_no_position(self):
        fn=next(n for n in tree('trade-tickets').body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loops=[n for n in fn.body if isinstance(n,ast.For) and 'cascade.get(tier_key)' in ast.unparse(n)]
        self.assertEqual(len(loops),1);ns={'cascade':self.packet(),'candidates':[],'seen':set()};execute(loops,ns)
        self.assertEqual(ns['candidates'],[]);self.assertEqual(ns['seen'],set())
    def test_prediction_loop_gets_no_cascade_forecast(self):
        loops=[n for n in ast.walk(tree('prediction-snapshotter')) if isinstance(n,ast.For) and isinstance(n.target,ast.Tuple) and
               [x.id for x in n.target.elts if isinstance(x,ast.Name)]==['tier_key','tier_label'] and any(isinstance(x,ast.Name) and x.id=='cascade' for x in ast.walk(n))]
        self.assertEqual(len(loops),1);ns={'cascade':self.packet(),'predictions':{},'today':'synthetic'};execute(loops,ns)
        self.assertEqual(ns['predictions'],{})
    def test_best_setups_loops_get_no_rank_contribution(self):
        loops=[n for n in ast.walk(tree('best-setups')) if isinstance(n,ast.For) and isinstance(n.iter,ast.BoolOp) and 'cascade.get(' in ast.unparse(n.iter)]
        self.assertEqual(len(loops),2);execute(loops,{'cascade':self.packet(),'add':forbidden,'normalize':forbidden})
    def test_existing_recalibration_loop_cannot_regenerate_candidates_from_empty_tiers(self):
        fn=next(n for n in tree('cascade-recalibrator').body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loops=[n for n in fn.body if isinstance(n,ast.For) and 'original_candidates = cascade.get(tier_key)' in ast.unparse(n)]
        self.assertEqual(len(loops),1);ns={'cascade':self.packet(),'output':{},'recalibrate_candidates':forbidden,'compute_rank_changes':forbidden}
        execute(loops,ns);self.assertEqual(ns['output'],{'alert_tier':[],'medium_tier':[],'laggards_hot_themes':[],'watch_tier':[]})


if __name__=='__main__':unittest.main(verbosity=2)
