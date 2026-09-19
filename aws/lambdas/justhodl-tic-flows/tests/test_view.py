import ast,copy,importlib.util,json,sys,textwrap,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/lambdas/justhodl-foreign-flows/tests')]
from test_store import fixture,Storage,StorageError,AT
import foreign_store as fs,foreign_research as fm,tic_view as v

def ready():
    store,inputs=fixture();store.objects[fm.CURRENT]=b'{"v":"1.5.0"}';store.objects[v.CURRENT]=b'{"regime":"STRONG_FOREIGN_DEMAND","all_legacy_fields":[1,2,3]}'
    with patch.object(fs,'collect',return_value=(inputs['originals'],inputs['acquisition_errors'],'2026-09-19')),patch.object(fs,'now',return_value=AT):result=fs.run(store,'b','fixture')
    key=result['replay']['manifest_key'];vi={'contract':'tic-view-inputs.v1','foreign_manifest':{'key':key,'sha256':key.rsplit('/',1)[-1][:-5]},'legacy':{'fixture':True}}
    return store,vi

class Tests(unittest.TestCase):
    def test_complete_view_retains_old_packet_and_exact_source_binding(self):
        store,_=ready();legacy=store.objects[v.CURRENT];result=v.run(store,'b',AT)
        self.assertTrue(result['published']);self.assertTrue(any(k.startswith(v.PRIVATE) and raw==legacy for k,raw in store.objects.items()))
        manifest=json.loads(v.reader(store,'b')(result['replay']['manifest_key']));out=v.replay(manifest,v.reader(store,'b'))
        self.assertEqual(out['n_holders'],24);self.assertEqual(out['individual']['belgium']['code'],'10251')
        self.assertEqual(out['individual']['belgium']['yoy_change_b'],0)
        self.assertEqual(out['individual']['belgium']['status'],'REPORTED_HOLDINGS');self.assertIsNone(out['composite_tic_stress'])
        self.assertEqual(out['net_purchases']['trailing_12mo_m'],1200);self.assertEqual(out['additional_independent_votes'],0)
        self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible'])
        store.objects[out['foreign_output']['key']]=b'{}'
        with self.assertRaises(ValueError):v.replay(manifest,v.reader(store,'b'))

    def test_live_pointer_cannot_disguise_a_different_source_output(self):
        store,_=ready();source=json.loads(store.objects[fm.CURRENT]);source['latest_month']='2026-06-01';store.objects[fm.CURRENT]=v.encoded(source)
        prior=store.objects[v.CURRENT]
        with self.assertRaises(ValueError):v.run(store,'b',AT)
        self.assertEqual(store.objects[v.CURRENT],prior)

    def test_exact_calendar_baseline_keeps_missing_month_unavailable(self):
        store,vi=ready();manifest=json.loads(store.objects[vi['foreign_manifest']['key']]);source=json.loads(store.objects[manifest['output']['key']]);group=source['groups']['10251']
        doc=json.loads(store.objects[group['history']['key']]);doc['series']['treas:pos']['rows']=[r for r in doc['series']['treas:pos']['rows'] if r['date']!='2025-07-01']
        def keep(doc,prefix):
            raw=v.encoded(doc);sha=v.digest(doc);key=prefix+sha+'.json';store.objects[key]=raw;return {'key':key,'sha256':sha,'bytes':len(raw)}
        group['history']=keep(doc,v.FOREIGN+'histories/');manifest['output']=keep(source,v.FOREIGN+'outputs/');manifest['output_sha256']=v.digest(source)
        vi['foreign_manifest']=keep(manifest,v.FOREIGN+'runs/');out=v.build(vi,v.reader(store,'b'),AT)
        self.assertEqual(out['individual']['belgium']['current_b'],1);self.assertIsNone(out['individual']['belgium']['yoy_change_b'])

    def test_new_view_timestamp_does_not_refresh_old_observations(self):
        store,vi=ready();out=v.build(vi,v.reader(store,'b'),'2026-09-21T00:00:00+00:00')
        self.assertEqual(out['quality']['status'],'stale_source');self.assertEqual(out['quality']['observation_age_days'],52)
        self.assertEqual(out['source_generated_at'],AT)

    def test_publication_refuses_source_or_output_time_regression(self):
        store=Storage();packet={'contract':v.CONTRACT,'generated_at':AT,'source_generated_at':AT}
        store.objects[v.CURRENT]=v.encoded({**packet,'generated_at':'2026-09-20T00:00:00Z'})
        self.assertFalse(v.publish(store,'b',packet))
        store.objects[v.CURRENT]=v.encoded({**packet,'other':1})
        with self.assertRaises(ValueError):v.publish(store,'b',packet)

    def test_http_never_invokes_collection_or_notifications(self):
        store=Storage();store.objects[v.CURRENT]=v.encoded({'contract':v.CONTRACT})
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store),'_fred_shim':types.SimpleNamespace()}):
            spec=importlib.util.spec_from_file_location('tic_view_active_test',SOURCE/'lambda_function.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with patch.object(v,'run',side_effect=AssertionError('HTTP collector forbidden')),patch.object(module,'maybe_telegram',side_effect=AssertionError('notifications forbidden')):
            result=module.lambda_handler({'httpMethod':'GET'},None)
        self.assertEqual(result['statusCode'],200);self.assertEqual(result['headers']['Cache-Control'],'no-store')

    def test_actual_official_pulse_never_promotes_descriptive_tic_zscores(self):
        source=(ROOT/'aws/lambdas/justhodl-official-pulse/source/lambda_function.py').read_text(encoding='utf-8')
        fn=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='dollar_leg');scope={}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'actual_dollar_leg','exec'),scope)
        ff={'holder_splits':{'lt_total':{'official':{'z_10y':-5}}},'signals':{'safe_haven':{'z_10y':-5}},'calls_eligible':True,'quality':{'status':'fresh'}}
        out=scope['dollar_leg']({},ff);self.assertEqual(out['status'],'UNKNOWN');self.assertEqual(out['legs_firing'],0)
        self.assertEqual(out['legs']['official_flows_monthly']['source_z'],-5)
        out=scope['dollar_leg']({'custody':{'status':'LIVE','z_13wchg_10y':-2}},ff)
        self.assertEqual(out['legs_firing'],0);self.assertEqual(out['firing'],[])
        self.assertEqual(out['legs']['custody_weekly']['source_z'],-2)

    def test_actual_world_map_keeps_holdings_context_out_of_scores(self):
        source=(ROOT/'aws/lambdas/justhodl-global-flow-desk/source/lambda_function.py').read_text(encoding='utf-8')
        block=source[source.index('    sv = _j("data/sovereign-fiscal.json"'):source.index('    fx = _j("data/polygon-fx-regime.json"')]
        packet={'holders':[{'country':'Japan','chg_12m_usd_b':500}], 'top_holders':[{'country':'Japan','current_b':1000}]}
        scope={'_j':lambda *args:packet};exec(compile(textwrap.dedent(block),'actual_world_map','exec'),scope)
        self.assertEqual(scope['tic_ctry'],{});self.assertEqual(scope['tic_context']['legacy_holder_inputs'],packet['holders'])
        self.assertEqual(scope['tic_context']['holdings'],packet['top_holders']);self.assertFalse(scope['tic_context']['calls_eligible'])

    def test_actual_cycle_readout_cannot_restore_a_legacy_tic_stress_score(self):
        source=(ROOT/'aws/lambdas/justhodl-cycle-clock/source/lambda_function.py').read_text(encoding='utf-8')
        values=[value for node in ast.walk(ast.parse(source)) if isinstance(node,ast.Dict) for key,value in zip(node.keys,node.values) if isinstance(key,ast.Constant) and key.value=='tic']
        self.assertEqual(len(values),1)
        packet={'composite_tic_stress':100,'regime':'DE_DOLLARIZATION','quality':{'status':'partial'}}
        scope={'ticflows':packet,'_get':lambda doc,key:doc.get(key)}
        out=eval(compile(ast.Expression(body=values[0]),'actual_cycle_tic','eval'),scope)
        self.assertIsNone(out['stress']);self.assertEqual(out['regime'],'MONITOR_ONLY');self.assertEqual(out['source_regime'],'DE_DOLLARIZATION')
        self.assertEqual(out['quality'],packet['quality']);self.assertFalse(out['calls_eligible'])

if __name__=='__main__':unittest.main(verbosity=2)
