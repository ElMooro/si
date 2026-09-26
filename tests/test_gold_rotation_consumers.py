from pathlib import Path
import ast,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
import gold_rotation_context as gate
from release_package_evidence import shared_imports
LEGACY={'state':'GOLD_BREAKOUT_RICH','signal_strength':1,'current_metrics':{'gld_20d_pct':12,'gdx_20d_pct':25,'uup_20d_pct':-8},'trade_tickets':[{'side':'LONG','size_pct_portfolio':3}]}
def native():
    p={'contract':'gold-rotation-original-research.v1','generated_at':'2020-01-01T00:00:00Z','call':None,'state':None,'signal_strength':None,
        'trade_tickets':[],'independent_investment_votes':0,**dict.fromkeys(gate.FLAGS,False)}
    p['replay']={'manifest_key':'data/gold-rotation-research/runs/'+'a'*64+'.json','output_sha256':hashlib.sha256(json.dumps(p,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()};return p
def source(name):return (ROOT/f'aws/lambdas/justhodl-{name}/source/lambda_function.py').read_text(encoding='utf-8')
def functions(name,wanted,scope=None):
    scope=scope or {};nodes=[n for n in ast.parse(source(name)).body if isinstance(n,ast.FunctionDef) and n.name in wanted]
    assert len(nodes)==len(wanted);exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope);return scope
class Tests(unittest.TestCase):
    def test_old_missing_and_self_promoted_packets_cannot_supply_scores_or_sizes(self):
        for p in (None,{},LEGACY,{**native(),'calls_eligible':True},{**native(),'state':'GOLD_BREAKOUT_RICH'}):
            out=gate.decision_view(p);self.assertEqual(out['current_metrics'],{});self.assertEqual(out['trade_tickets'],[])
            self.assertIsNone(out['state']);self.assertFalse(out['research_context']['native_reference_available'])
    def test_reference_digest_has_honest_replay_and_freshness_scope(self):
        p=native();out=gate.context(p);self.assertTrue(out['native_reference_available']);self.assertTrue(out['output_digest_checked'])
        self.assertFalse(out['original_provider_replay_performed_by_consumer']);self.assertFalse(out['current_freshness_verified_by_consumer'])
        p['changed']=True;self.assertFalse(gate.context(p)['native_reference_available'])
    def test_actual_signal_board_abstains(self):
        from signal_board_native_test_support import assert_abstention
        assert_abstention('data/gold-equity-rotation.json', (LEGACY,native(),{},None))
    def test_actual_allocator_cannot_size_a_gold_sleeve_from_unqualified_momentum(self):
        for p in (LEGACY,native()):
            scope=functions('master-allocator',{'gather_signals','clamp'},{'read_json':lambda key:p if key==gate.CURRENT else {}})
            self.assertNotIn('gold_rotation',scope['gather_signals']())
    def test_actual_narrative_input_retains_reference_without_legacy_flow_labels(self):
        tree=ast.parse(source('morning-intelligence'))
        node=next(n for n in ast.walk(tree) if isinstance(n,ast.Lambda) and any(a.arg=='ger' for a in n.args.args))
        scope={'data':{'gold_equity_rotation':LEGACY}}
        expression=ast.fix_missing_locations(ast.Expression(body=ast.Call(func=node,args=[],keywords=[])))
        value=eval(compile(expression,'morning-intelligence','eval'),scope)
        self.assertIsNone(value['flow_gold_20d']);self.assertIsNone(value['flow_metals_state']);self.assertFalse(value['gold_research_context']['native_reference_available'])
    def test_each_actual_consumer_bundles_the_shared_boundary(self):
        for name in ('signal-board','master-allocator','morning-intelligence'):
            if name=='signal-board':
                from signal_board_native_test_support import assert_compiler_pin
                assert_compiler_pin()
                continue
            paths=list((ROOT/f'aws/lambdas/justhodl-{name}/source').glob('*.py'))
            self.assertIn('gold_rotation_context.py',[p.name for p in shared_imports(ROOT,paths)])

if __name__=='__main__':unittest.main(verbosity=2)
