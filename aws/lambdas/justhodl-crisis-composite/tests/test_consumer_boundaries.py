"""Execute actual consumer functions/blocks offline; never invoke a live consumer."""
import ast,json,sys,unittest
from pathlib import Path
from typing import List
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'aws/shared'))
import crisis_authority as authority


def tree(engine,file='lambda_function.py'):
    path=ROOT/'aws/lambdas'/('justhodl-'+engine)/'source'/file
    return ast.parse(path.read_text(encoding='utf-8'))


def actual(engine,names,scope=None,file='lambda_function.py'):
    env={'List':List,**(scope or {})}
    nodes=[n for n in tree(engine,file).body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in names]
    assert len(nodes)==len(names)
    for node in nodes:node.decorator_list=[]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-'+engine,'exec'),env)
    return env


def block(nodes,variable,length,scope):
    start=next(i for i,n in enumerate(nodes) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id==variable)
    exec(compile(ast.Module(body=nodes[start:start+length],type_ignores=[]),'actual-consumer-block','exec'),scope)


class Boundaries(unittest.TestCase):
    SPOOF={'generated_at':'2026-09-20T09:30:00Z','master_crisis_score':99,'composite_score':99,'score':99,'composite':99,'level':99,
           'defcon_level':1,'defcon_name':'ACUTE','trend':'CRISIS','playbook':'sell all','calls_eligible':True,'sizing_eligible':True,
           'qualification':'qualified','replay':{'verified':True},'components':[{'available':True,'score':99}],'components_available':14}

    def test_self_asserted_flags_and_aliases_never_qualify(self):
        for packet in (self.SPOOF,{},None):
            self.assertIsNone(authority.qualified_score(packet));clean=authority.decision_view(packet)
            for key in ('score','master_crisis_score','composite_score','defcon_level','playbook','composite','level'):self.assertIsNone(clean.get(key))
            self.assertFalse(clean['sizing_eligible']);self.assertEqual(clean['components'],[])

    def test_actual_allocator_and_conviction_do_not_create_votes(self):
        calls=[];f=actual('allocator',['rule_crisis_composite'],{'fs3':lambda key:self.SPOOF,'add':lambda *a:calls.append(a)})
        f['rule_crisis_composite']({},{});self.assertEqual(calls,[])
        f=actual('conviction-engine',['n_crisis_composite'])
        self.assertIsNone(f['n_crisis_composite'](self.SPOOF)[0])

    def test_actual_desk_missing_inputs_are_unavailable_not_neutral(self):
        f=actual('desk-allocator',['read_regime'],{'get_json':lambda k:self.SPOOF if 'crisis' in k else {},'clamp':lambda x,a,b:max(a,min(x,b))})
        out=f['read_regime']();self.assertIsNone(out['blended_risk_axis']);self.assertIsNone(out['crisis_score'])
        self.assertIn('UNAVAILABLE',json.dumps(out))

    def test_actual_allocator_both_primary_and_defensive_alias_paths(self):
        t=tree('master-allocator');f=next(n for n in t.body if isinstance(n,ast.FunctionDef) and n.name=='gather_signals')
        env={'read_json':lambda k:self.SPOOF,'out':{}};block(f.body,'cc',3,env);self.assertEqual(env['out'],{})
        owner=next(n for n in t.body if isinstance(n,ast.FunctionDef) and n.name!='gather_signals' and any(isinstance(x,ast.Assign) and isinstance(x.targets[0],ast.Name) and x.targets[0].id=='cc' for x in n.body))
        env={'_ba_s3json':lambda k:self.SPOOF,'reasons':[]};block(owner.body,'cc',2,env);self.assertEqual(env['reasons'],[])

    def test_actual_router_does_not_substitute_zero_or_hundred(self):
        f=actual('regime-conditional-router',['detect_dollar_smile_left','detect_dollar_smile_right'],{'safe_get':lambda d,k:d.get(k) if isinstance(d,dict) else None})
        for name,args in [('detect_dollar_smile_left',({'score':70},{},self.SPOOF)),('detect_dollar_smile_right',({'score':70},{},self.SPOOF,{}))]:
            value,e=f[name](*args);self.assertEqual(value,0);self.assertIsNone(e['crisis'])

    def test_actual_fanout_cannot_send_initial_or_change_alert(self):
        f=actual('streaming-fanout',['_extract_summary','_is_meaningful_delta'])
        engine={'name':'crisis_composite','summary_fields':list(self.SPOOF)};out=f['_extract_summary'](engine,self.SPOOF)
        self.assertNotIn('master_crisis_score',out)
        for old in (None,{'score':0}):self.assertEqual(f['_is_meaningful_delta'](engine,old,out),(False,'unqualified_crisis_composite'))

    def test_actual_prepump_preserves_prior_alert_state(self):
        state={'last_crisis_regime':'ACUTE_CRISIS'};f=actual('prepump-alerts-router',['check_crisis_composite'],{'_read_json':lambda k:self.SPOOF})
        self.assertEqual(f['check_crisis_composite'](state),[]);self.assertEqual(state,{'last_crisis_regime':'ACUTE_CRISIS'})

    def test_actual_near_miss_skips_before_snapshot_read(self):
        f=next(n for n in tree('near-miss-monitor').body if isinstance(n,ast.FunctionDef) and n.name=='handler')
        loop=next(n for n in f.body if isinstance(n,ast.For) and isinstance(n.iter,ast.Name) and n.iter.id=='NEAR_MISS_CONFIGS')
        env={'NEAR_MISS_CONFIGS':[{'signal_type':'crisis','snapshot_key':'data/crisis-composite.json'}],'diagnostics':[],'snapshot_cache':{}}
        exec(compile(ast.Module(body=[loop],type_ignores=[]),'actual-near-miss','exec'),env)
        self.assertTrue(env['diagnostics'][0]['skipped']);self.assertEqual(env['snapshot_cache'],{})

    def test_actual_morning_distillation_cannot_restore_cached_score(self):
        node=next(n for n in ast.walk(tree('morning-intelligence')) if isinstance(n,ast.Lambda) and 'crisis_composite' in ast.unparse(n.args))
        function=eval(compile(ast.Expression(body=node),'actual-morning','eval'),{'data':{'crisis_composite':self.SPOOF}})
        out=function();self.assertIsNone(out['crisis_score']);self.assertIsNone(out['crisis_defcon']);self.assertIsNone(out['crisis_playbook'])

    def test_actual_calibrator_ignores_current_and_all_history_sources(self):
        f=next(n for n in tree('calibration-fleet').body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loops=[n for n in f.body if isinstance(n,ast.For) and isinstance(n.iter,ast.Name) and n.iter.id=='REGISTRY']
        env={'REGISTRY':[{'name':'crisis_composite','label':'Crisis','direction':'stress','source_key':'data/crisis-composite.json','score_path':['master_crisis_score']}],
             'read_json':lambda k:self.SPOOF,'current_scores':{},'engine_status':[],'engines_out':[],'weight_props':{}}
        exec(compile(ast.Module(body=loops,type_ignores=[]),'actual-calibrator','exec'),env)
        self.assertEqual(env['current_scores'],{});self.assertEqual(env['weight_props'],{});self.assertEqual(env['engines_out'][0]['quality_rating'],'UNQUALIFIED')

    def test_actual_orthogonality_excludes_legacy_crisis_history(self):
        old=[{'date':'2026-09-01','scores':{'crisis_composite':90,'defcon':1,'gsi_total':99,'other':3}}]
        f=next(n for n in tree('signal-orthogonality').body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        node=next(n for n in f.body if isinstance(n,ast.Assign) and 'crisis_authority' in ast.unparse(n))
        env={'snaps':old};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-orthogonality','exec'),env)
        self.assertEqual(env['snaps'][0]['scores'],{'other':3});self.assertIn('crisis_composite',old[0]['scores'])

    def test_actual_adapter_cannot_emit_rows_even_with_spoofed_authority(self):
        import jh_adapters
        adapter=jh_adapters.CrisisCompositeAdapter({'engine_id':'crisis_composite'}, {})
        self.assertFalse(adapter.validate_source(self.SPOOF));self.assertEqual(list(adapter.rows(self.SPOOF)),[])

    def test_native_research_schema_is_valid_without_conferring_authority(self):
        p={'contract':'crisis-research.v1','measurements':{'SOFR':{}},'quality':{'status':'degraded'},
           'replay':{'manifest_key':'data/crisis-research/runs/'+'a'*64+'.json','output_sha256':'b'*64},
           **dict.fromkeys(('calls_eligible','sizing_eligible','execution_eligible','forecast_eligible'),False)}
        self.assertIsNone(authority.research_error(p));self.assertIsNone(authority.qualified_score(p))
        p['sizing_eligible']=True;self.assertIn('authority',authority.research_error(p))

    def test_actual_risk_policy_ignores_crisis_levels_in_both_readers(self):
        for engine,file,name in [('khalid','scoring.py','risk_policy'),('khalid-risk','risk_engine.py','base_policy')]:
            f=actual(engine,[name],{'number':lambda x:float(x) if type(x) in (float,int) else None,
                'first_number':lambda *xs:next((x for x in xs if type(x) in (float,int)),None)},file)[name]
            gate={'posture':'RISK_ON','composite':1,'sizing_multiplier':0.5}
            a=f(gate,self.SPOOF,[]);b=f(gate,{**self.SPOOF,'defcon_level':5,'master_crisis_score':0},[])
            self.assertEqual(a,b);self.assertIsNone(a['crisis_defcon'])


if __name__=='__main__':unittest.main()
