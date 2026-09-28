"""Isolated source-level compatibility only; no consumer outputs or native I/O."""
from pathlib import Path
import ast,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tests/ops'))
from test_velocity_evidence import captures,AT
from velocity_evidence import build


def tree(name):return ast.parse((ROOT/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py').read_bytes())
def forbidden(*a,**kw):raise AssertionError('No learning, notification or state mutation')


class Tests(unittest.TestCase):
    def packet(self):return build(*captures(),AT)
    def test_alert_transition_block_has_no_new_alert_or_state_mutation(self):
        node=next(n for n in tree('prepump-alerts-router').body if isinstance(n,ast.FunctionDef) and n.name=='check_velocity_transitions')
        p=self.packet();reads=[]
        def read(key):reads.append(key);return p
        ns={'List':list,'_read_json':read,'_is_new':forbidden,'_mark_alerted':forbidden}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'<synthetic transition only>','exec'),ns)
        state={'preserve':'whole old state'};self.assertEqual(ns[node.name](state),[])
        self.assertEqual(state,{'preserve':'whole old state'});self.assertEqual(reads,['data/velocity-acceleration.json'])
    def test_prediction_loop_gets_no_forecast_from_descriptive_statistics(self):
        loops=[n for n in ast.walk(tree('prediction-snapshotter')) if isinstance(n,ast.For) and isinstance(n.target,ast.Tuple) and
               [x.id for x in n.target.elts if isinstance(x,ast.Name)]==['tier_key','tier_label'] and
               any(isinstance(x,ast.Name) and x.id=='velocity' for x in ast.walk(n))]
        self.assertEqual(len(loops),1);ns={'velocity':self.packet(),'predictions':{},'today':'synthetic'}
        exec(compile(ast.Module(body=loops,type_ignores=[]),'<synthetic feature loop only>','exec'),ns)
        self.assertEqual(ns['predictions'],{})
    def test_convergence_original_action_path_receives_no_tickers(self):
        node=next(n for n in tree('convergence-radar').body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ENGINE_EXTRACTORS' for t in n.targets))
        spec=ast.literal_eval(node.value)['velocity-acceleration'];self.assertEqual(spec['path'],'actionable_tickers')
        self.assertEqual(self.packet()[spec['path']],[])
    def test_theme_cascade_original_tiers_are_preserved_empty(self):
        source=tree('theme-cascade');keys={n.args[0].value for n in ast.walk(source) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and
            n.func.attr=='get' and isinstance(n.func.value,ast.Name) and n.func.value.id=='velocity' and n.args and isinstance(n.args[0],ast.Constant)}
        self.assertEqual(keys,{'confirmed_today','fresh_fires','aging','emerging','watch'})
        for key in keys:self.assertEqual(self.packet()[key],[])


if __name__=='__main__':unittest.main(verbosity=2)
