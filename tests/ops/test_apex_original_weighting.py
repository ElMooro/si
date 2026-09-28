"""Draft isolated counterexamples against preserved source; no native/ledger I/O."""
from pathlib import Path
import ast,math,json,hashlib,unittest
ROOT=Path(__file__).resolve().parents[2]
TREE=ast.parse((ROOT/'tests/fixtures/pre-momentum-leaders-apex-fusion.py.txt').read_bytes())


def weights(rows):
    ns={'_rd':lambda key:{'scorecard':rows}}
    nodes=[n for n in TREE.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('BASE_W','SCORECARD_TOKENS') for t in n.targets) or
           isinstance(n,ast.FunctionDef) and n.name=='learned_weights']
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated synthetic weighting>','exec'),ns)
    return ns,ns['learned_weights']()


def fuse(components):
    handler=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
    loop=next(n for n in handler.body if isinstance(n,ast.For) and isinstance(n.target,ast.Tuple) and [e.id for e in n.target.elts]==['tk','e'])
    ns={'book':{'SYNA':{'components':components,'evidence':{},'price':100}},'rows':[],
        'W':dict.fromkeys(components,1),'inv':{'active':False}}
    exec(compile(ast.Module(body=[loop],type_ignores=[]),'<isolated synthetic fusion>','exec'),ns)
    return ns['rows'][0]


class Tests(unittest.TestCase):
    def test_complete_original_matches_original_actual_package_capture(self):
        raw=(ROOT/'tests/fixtures/pre-momentum-leaders-apex-fusion.py.txt').read_bytes()
        baseline=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-apex-fusion']
        self.assertEqual(len(raw),baseline['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),baseline['sha256'])
    def test_nan_performance_multiplier_becomes_maximum_boost(self):
        ns,(w,src)=weights([{'signal_type':'momentum','status':'PROMOTED','n_scored':20,'performance_multiplier':float('nan')}])
        self.assertEqual(w['momentum'],round(ns['BASE_W']['momentum']*1.5,3));self.assertTrue(math.isnan(src['momentum']['multiplier']))
    def test_duplicating_correlated_components_inflates_score_and_tier(self):
        a=fuse({'momentum':70});b=fuse({'momentum':70,'pump':70,'flow':70})
        self.assertEqual(a['apex_score'],54.6);self.assertEqual(b['apex_score'],81.2)
        self.assertEqual(b['tier'],'LIFTOFF')
    def test_matching_scores_from_unqualified_inputs_have_no_eligibility_or_clock_gate(self):
        p=fuse({'momentum':100,'flow':100,'insider':100})
        self.assertEqual(p['apex_score'],100);self.assertEqual(p['tier'],'LIFTOFF');self.assertNotIn('forecast_qualified',p)
    def test_equal_count_scorecard_rows_change_weight_when_reordered(self):
        a={'signal_type':'momentum','status':'PROMOTED','n_scored':20,'performance_multiplier':.5}
        b=dict(a,performance_multiplier=1.5)
        self.assertNotEqual(weights([a,b])[1][0]['momentum'],weights([b,a])[1][0]['momentum'])
    def test_future_and_explicitly_unqualified_scorecard_still_promotes(self):
        ns,(w,src)=weights([{'signal_type':'momentum','status':'PROMOTED','n_scored':20,'performance_multiplier':1.5,
                            'as_of':'2099-01-01','forecast_qualified':False,'out_of_sample':False}])
        self.assertEqual(w['momentum'],round(ns['BASE_W']['momentum']*1.5,3));self.assertTrue(src)
    def test_boolean_and_negative_hit_rates_can_trigger_inversion(self):
        fn=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='tier_inversion')
        for hr in (False,-999):
            def read(key):return {'by_tier_stats':{'ALERT_TIER':{'n':10,'hit_rate_pct':hr}}} if key.endswith('log.json') else {'alert_tier':[{'ticker':'SYNA'}]}
            ns={'_rd':read};exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated inversion>','exec'),ns)
            self.assertIs(ns['tier_inversion']()['active'],True)
    def test_score_is_relabelled_probability_without_calibration(self):
        item=next(n.value for n in ast.walk(TREE) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='item' for t in n.targets))
        expr=next(v for k,v in zip(item.keys,item.values) if isinstance(k,ast.Constant) and k.value=='confidence')
        r=fuse({'momentum':70,'flow':70,'insider':70})
        out=eval(compile(ast.Expression(expr),'<isolated confidence expression>','eval'),{'r':r})
        self.assertEqual(out,{'N':'0.812'})


if __name__=='__main__':unittest.main(verbosity=2)
