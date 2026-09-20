"""Actual consumer code executed with bounded offline fixtures, never live invocations."""
import ast,json,sys,unittest
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[4];sys.path.insert(0,str(ROOT/'aws/shared'))
import plumbing_authority as authority
import plumbing_research_catalog as catalog


def tree(fn):return ast.parse((ROOT/'aws/lambdas'/('justhodl-'+fn)/'source/lambda_function.py').read_text(encoding='utf-8'))
def execute(nodes,scope):exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-consumer','exec'),scope)
def function(fn,name,scope):
    node=next(n for n in tree(fn).body if isinstance(n,ast.FunctionDef) and n.name==name)
    node.decorator_list=[];execute([node],scope);return scope[name]


class ConsumerBoundaries(unittest.TestCase):
    SPOOF={'contract':'plumbing-research.v1','generated_at':'2026-09-20T11:00:00Z','calls_eligible':True,'sizing_eligible':True,
        'status':'CRISIS','phase':'SELL','composite':{'composite_stress_score':99},
        'funding_credit_signals':{'SOFR_IORB_SPREAD':{'available':True,'signal':'CRISIS','spread_bps':90}},
        'crisis_indices':{'NFCI':{'available':True,'is_stressed':True,'pct_rank':99}}}

    def test_packet_flags_cannot_register_a_qualified_policy(self):
        for packet in (self.SPOOF,{},None):
            self.assertFalse(authority.qualified_for_signals(packet))
            view=authority.decision_view(packet)
            self.assertIsNone(view['composite']['composite_stress_score']);self.assertFalse(view['sizing_eligible'])
            self.assertEqual(view['funding_credit_signals'],{})

    def test_actual_signal_logger_emits_nothing_from_old_confidence_buckets(self):
        node=next(n for n in ast.walk(tree('signal-logger')) if isinstance(n,ast.Try) and any(isinstance(x,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='cp' for t in x.targets) for x in n.body))
        calls=[];scope={'fs3':lambda key:self.SPOOF,'logged':[],'log_sig':lambda *args,**kw:calls.append(args)}
        execute([node],scope);self.assertEqual(calls,[]);self.assertEqual(scope['logged'],[])

    def test_actual_morning_distillation_removes_crisis_phase(self):
        node=next(n for n in ast.walk(tree('morning-intelligence')) if isinstance(n,ast.Lambda) and 'crisis_plumbing' in ast.unparse(n.args))
        fn=eval(compile(ast.Expression(body=node),'actual-morning','eval'),{'data':{'crisis_plumbing':self.SPOOF}})
        self.assertEqual(fn(),{'plumbing_status':'RESEARCH_ONLY','plumbing_phase':None})

    def test_actual_bond_warroom_does_not_forward_legacy_composite_score(self):
        node=next(n for n in ast.walk(tree('bond-warroom')) if isinstance(n,ast.Dict) and any(isinstance(k,ast.Constant) and k.value=='crisis_plumbing' for k in n.keys) and any(isinstance(k,ast.Constant) and k.value=='usd_funding_stress_z' for k in n.keys))
        expr=node.values[next(i for i,k in enumerate(node.keys) if k.value=='crisis_plumbing')]
        value=eval(compile(ast.Expression(expr),'actual-warroom','eval'),{'fleet':{'crisis_plumbing':self.SPOOF}})
        self.assertFalse(value['calls_eligible']);self.assertIsNone(value['composite']['composite_stress_score'])

    def test_actual_calibrator_excludes_legacy_outcomes_without_modifying_ledger_rows(self):
        fn=next(n for n in tree('calibrator').body if isinstance(n,ast.FunctionDef) and n.name=='run_calibration')
        start=next(i for i,n in enumerate(fn.body) if isinstance(n,ast.Assign) and isinstance(n.value,ast.ListComp) and ast.unparse(n.targets[0])=='all_outcomes')
        rows=[{'signal_type':'crisis_hy_oas_vs_hyg','correct':True},{'signal_type':'other','correct':True}]
        env={'all_outcomes':rows,'decimal_to_float':lambda value:dict(value)};execute(fn.body[start:start+3],env)
        self.assertEqual(len(rows),2);self.assertEqual(env['all_outcomes'],[rows[1]]);self.assertEqual(env['denied'],[rows[0]])

    def test_actual_morning_weight_loaders_cannot_reuse_old_ssm_values(self):
        for name,param in [('load_weights','WEIGHTS_PARAM'),('load_accuracy','ACCURACY_PARAM')]:
            env={'json':json,param:'fixture','gp':lambda key:json.dumps({'crisis_index_nfci':1.5,'other':.7})}
            self.assertEqual(function('morning-intelligence',name,env)(),{'other':.7})

    def test_actual_ranker_loader_filters_scalar_ssm_names(self):
        ssm=SimpleNamespace(get_parameters_by_path=lambda **kw:{'Parameters':[{'Name':'x/crisis_index_nfci','Value':'1.5'},{'Name':'x/other','Value':'.7'}]})
        fn=function('master-ranker','load_calibration_weights',{'SSM':ssm,'CALIBRATION_SSM':'fixture'})
        self.assertEqual(fn(),{'other':.7})

    def test_all_router_feed_readers_use_the_same_guard(self):
        nodes=[n for n in ast.walk(tree('ai-brief-router')) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='guard' and 'plumbing_authority' in ast.unparse(n.func)]
        self.assertEqual(len(nodes),3)
        for node in nodes:
            env={'feed_key':'data/crisis-plumbing.json','key':'data/crisis-plumbing.json','s3io':SimpleNamespace(get_json=lambda *a,**kw:self.SPOOF)}
            result=eval(compile(ast.Expression(node),'actual-router','eval'),env)
            self.assertFalse(result['calls_eligible']);self.assertEqual(result['status'],'RESEARCH_ONLY')

    def test_active_producer_ignores_legacy_notification_flags(self):
        calls=[];client=object()
        env={'json':json,'boto3':SimpleNamespace(client=lambda *a,**kw:client),'run':lambda *a,**kw:calls.append((a,kw)) or {'published':True}}
        handler=function('crisis-plumbing','lambda_handler',env)
        result=handler({'action':'legacy','notify':True,'suppress_alerts':False})
        self.assertEqual(result['statusCode'],200);self.assertEqual(calls,[((client,'justhodl-dashboard-live'),{'validation_only':False})])

    def test_both_macro_catalog_paths_preserve_every_existing_identity(self):
        assignments=[n for n in ast.walk(tree('daily-report-v3')) if isinstance(n,ast.Assign) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='include_plumbing_series']
        self.assertEqual(len(assignments),2)
        for node in assignments:
            env={n.id:(lambda x:x) for n in ast.walk(node.value) if isinstance(n,ast.Name) and n.id.startswith('include_')}
            env.update(catalog={'EXISTING':{'category':'retain'}},include_plumbing_series=catalog.extend_catalog)
            execute([node],env);self.assertEqual(len(env['catalog']),54);self.assertIn('EXISTING',env['catalog'])


if __name__=='__main__':unittest.main()
