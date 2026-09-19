import ast
from datetime import datetime,timezone,timedelta
import io,json,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
from risk_gate_authority import context,read

ROOT=Path(__file__).resolve().parents[3]
NOW=datetime(2026,9,18,20,tzinfo=timezone.utc)


def functions(engine,names,extra=None):
    tree=ast.parse((ROOT/'aws/lambdas'/engine/'source/lambda_function.py').read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names),(engine,names)
    space=dict(extra or {});exec(compile(ast.Module(body=nodes,type_ignores=[]),engine,'exec'),space)
    return space


class Tests(unittest.TestCase):
    def test_flags_and_legacy_multiplier_never_self_certify_a_model(self):
        for value in (None,0,1,True,float('nan'),-1):
            doc={'generated_at':NOW.isoformat(),'posture':'RISK_ON','composite':2,'sizing_multiplier':value,
                 'calls_eligible':True,'sizing_eligible':True,'model_status':'VERIFIED'}
            out=context(doc,NOW);self.assertIsNone(out['sizing_multiplier']);self.assertFalse(out['allows_new_entries'])
            self.assertFalse(out['forced_liquidation']);self.assertFalse(out['calls_eligible'])
            self.assertEqual(out['source_status'],'current_packet')
            json.dumps(out,allow_nan=False)
            if value==0 and not isinstance(value,bool):self.assertEqual(out['declared_sizing_multiplier'],0)
        for stamp in ('2020-01-01T00:00:00Z','2099-01-01T00:00:00Z','2026-09-18T20:00:00',None):
            self.assertEqual(context({'generated_at':stamp},NOW)['source_status'],'invalid_or_expired')

    def test_actual_ranker_readers_do_not_cache_across_invocations(self):
        class Client:
            def __init__(self):self.reads=0
            def get_object(self,**kw):
                self.reads+=1
                doc={'generated_at':datetime.now(timezone.utc).isoformat(),'posture':'RISK_ON','sizing_multiplier':1}
                return {'Body':io.BytesIO(json.dumps(doc).encode())}
        for engine in ('justhodl-best-setups','justhodl-master-ranker','justhodl-opportunity-engine'):
            client=Client();fake=types.SimpleNamespace(client=lambda _:client)
            with patch.dict(sys.modules,{'boto3':fake}):
                f=functions(engine,['_risk_gate_doc'])['_risk_gate_doc']
                for _ in range(2):self.assertIsNone(f()['sizing_multiplier'])
            self.assertEqual(client.reads,2)
            tree=ast.parse((ROOT/'aws/lambdas'/engine/'source/lambda_function.py').read_text(encoding='utf-8'))
            handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
            calls=[n for n in ast.walk(handler) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='_risk_gate_doc']
            self.assertEqual(len(calls),1,'one consistent authority read per invocation')

    def test_actual_quantum_path_abstains_and_never_invents_a_full_size(self):
        space=functions('justhodl-quantum-desk',['risk_layer','verdict_for','build_money_map'],
            {'deep_find_setups':lambda _: [{'ticker':'TEST'}], 'dig':lambda d,*keys:next((d[k] for k in keys if k in d),None),
             'num':lambda v:float(v) if v is not None else None,'clamp':lambda v:max(0,min(1,v)),
             'QUAD_BONUS':{},'classify_asset':lambda _:None,'os':types.SimpleNamespace(environ={}),
             'build_evidence':lambda *a: ({},{}),
             'MM_W':{'conviction':.25,'quadrant':.25,'class_gate':.25,'flow':.25}})
        for doc in (None,{'generated_at':NOW.isoformat(),'posture':'RISK_ON','sizing_multiplier':1}):
            risk=space['risk_layer'](doc)
            self.assertEqual(space['verdict_for'](.99,{'discount':1},risk),'ABSTAIN')
            result=space['build_money_map']({},[],risk)
            self.assertIsNone(result[0][0]['size_hint_x']);self.assertFalse(result[0][0]['sizing_eligible'])
        risk={'sizing_multiplier':0,'allows_new_entries':False}
        self.assertEqual(space['build_money_map']({},[],risk)[0][0]['size_hint_x'],0)
        self.assertEqual(space['verdict_for'](.99,{'discount':1},risk),'ABSTAIN')

    def test_quantum_veto_requires_model_qualification_not_threshold_recovery(self):
        space=functions('justhodl-quantum-desk',['build_risk_panel'],{'_rows_with':lambda *a:[]})
        row=space['build_risk_panel']({},context(None,NOW),None)['veto_stack'][0]
        self.assertTrue(row['active']);self.assertIsNone(row['sizing_x'])
        self.assertIn('out-of-sample evidence',row['flips_when'])
        self.assertNotIn('composite recovers',row['flips_when'])

    def test_market_machine_cannot_recover_an_unqualified_nested_score(self):
        doc={'generated_at':datetime.now(timezone.utc).isoformat(),'posture':'UNAVAILABLE',
             'composite':None,'fleet':{'state':'RISK_ON','score':99}}
        space=functions('justhodl-market-machine',['pillar_forced'],
            {'s3_json_multi':lambda keys:(doc if keys[0]=='data/risk-gate.json' else None,keys[0]),
             'fred_series':lambda *a:[],
             'walk_find':lambda *a:99,'walk_find_str':lambda *a:'RISK_ON',
             'squash_z':lambda *a:99,'contrib':lambda *a: a})
        self.assertEqual(space['pillar_forced'](),[])


if __name__=='__main__':unittest.main()
