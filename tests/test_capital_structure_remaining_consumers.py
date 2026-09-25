from pathlib import Path
from io import BytesIO
from unittest.mock import Mock
from datetime import datetime,timedelta,timezone
import ast,hashlib,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from test_capital_structure_consumers import functions,tree,LEGACY


class Tests(unittest.TestCase):
    def test_whole_predecessors_are_preserved(self):
        proof=json.loads((ROOT/'tests/fixtures/capital-structure-remaining-consumer-migration.json').read_bytes())
        for entry in proof['files']:
            raw=(ROOT/entry['predecessor']).read_bytes()
            self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(entry['bytes'],entry['sha256']))

    def test_deal_dilution_join_does_not_return_a_clean_or_risky_classification(self):
        client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps(LEGACY).encode())}
        scope={'json':json,'s3':client,'S3_BUCKET':'test'}
        functions('justhodl-deal-scanner',{'load_shareflows'},scope)
        self.assertEqual(scope['load_shareflows'](),{})
        client.put_object.assert_not_called()
        assignments=[n for n in ast.walk(tree('justhodl-deal-scanner')) if isinstance(n,ast.Assign)
            and any(isinstance(t,ast.Name) and t.id=='diluting' for t in n.targets)]
        self.assertEqual(len(assignments),1);self.assertIsNone(ast.literal_eval(assignments[0].value))

    def test_industry_score_excludes_missing_dilution_weight_and_reports_its_ineligibility(self):
        names=['ABC'+str(i) for i in range(6)]
        packets={'data/universe.json':{'stocks':[{'symbol':n,'industry':'Widgets','sector':'Industrials','market_cap':1e9} for n in names]},
            'data/eps-revision-velocity.json':{'rows':[{'ticker':n,'score':1} for n in names]},
            'data/fundamental-census-matrix.json':{'tickers':names,'cols':{'conviction_score':[50]*6,'risk_score':[1]*6}},
            'data/share-flows.json':{'tickers':{n:{'sh_yoy_pct':500} for n in names}}}
        client=Mock();scope={'json':json,'time':time,'datetime':datetime,'timedelta':timedelta,'timezone':timezone,
            'BUCKET':'test','OUT_KEY':'data/industry-boom.json','HIST_KEY':'history','s3':client,'_g':lambda key:packets.get(key)}
        functions('justhodl-industry-boom',{'_rows_with','_pct','lambda_handler'},scope)
        scope['lambda_handler']()
        writes={c.kwargs['Key']:json.loads(c.kwargs['Body']) for c in client.put_object.call_args_list}
        output=writes['data/industry-boom.json'];self.assertEqual(len(output['league']),1)
        self.assertIsNone(output['league'][0]['comp']['dilution_share'])
        self.assertEqual(output['league'][0]['coverage_w'],105)
        self.assertFalse(output['coverage']['sources_ok']['share_flows'])
        self.assertFalse(output['input_eligibility']['ownership_dilution'])

    def test_impact_and_spx_input_boundaries_cannot_restore_legacy_shares_or_traps(self):
        for name,reader in (('justhodl-impact-graph','_get_json'),('justhodl-spx-beaters','_g')):
            nodes=[n for n in ast.walk(tree(name)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                and n.func.attr=='decision_view' and ast.unparse(n.func.value)=="__import__('capital_structure_context')"]
            self.assertEqual(len(nodes),1)
            output=eval(compile(ast.Expression(nodes[0]),name,'eval'),{reader:lambda key:LEGACY})
            self.assertEqual(output['tickers'],{});self.assertEqual(output['rows'],[])
            self.assertFalse(output['research_context']['buyback_signal_qualified'])

    def test_spx_never_calls_paid_ai_or_promotes_cached_or_fallback_probabilities(self):
        # These exact deployed functions must work with no SDK, credential,
        # network function or model cache available in their execution scope.
        scope=functions('justhodl-spx-beaters',{'ai_call','ai_verdict'},{})
        for row in ({'t':'ABC','score':100,'odds_base_26w_pct':99},{}):
            self.assertIsNone(scope['ai_call'](row,{},'momentum'))
            result=scope['ai_verdict'](row,'momentum',{'ABC':{'stance':'BUY','odds_beat_spx_26w_pct':99}})
            self.assertEqual(result['stance'],'WAIT');self.assertIsNone(result['odds_beat_spx_26w_pct'])
            self.assertIsNone(result['downside_risk_pct']);self.assertFalse(result['sizing_eligible'])
        source=ast.unparse(tree('justhodl-spx-beaters'))
        self.assertNotIn('api.anthropic.com',source);self.assertNotIn('ANTHROPIC_KEY',source)

    def test_all_spx_rows_withhold_forward_odds_and_preserve_recorded_cohort_context(self):
        nodes=[n for n in ast.walk(tree('justhodl-spx-beaters')) if isinstance(n,ast.For)
            and ast.unparse(n.target)=='rows' and ast.unparse(n.iter)=='out_buckets.values()']
        self.assertEqual(len(nodes),1)
        rows=[{'odds_base_26w_pct':99},{'odds_base_26w_pct':0},{}]
        scope={'out_buckets':{'large':rows,'empty':[]}}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-spx-publisher-boundary','exec'),scope)
        for row in rows:
            self.assertIsNone(row['odds_base_26w_pct']);self.assertFalse(row['forecast_qualified'])
            self.assertFalse(row['sizing_eligible'])
        self.assertEqual([r['historical_cohort_rate_unqualified_pct'] for r in rows],[99,0,None])

    def test_missing_industry_config_does_not_reset_the_accepted_native_runtime(self):
        config=json.loads((ROOT/'aws/lambdas/justhodl-industry-boom/config.json').read_bytes())
        self.assertEqual((config['timeout'],config['memory']),(180,1024))
        self.assertFalse(config['inherit_env']);self.assertNotIn('schedule',config)
        spx=json.loads((ROOT/'aws/lambdas/justhodl-spx-beaters/config.json').read_bytes())
        self.assertFalse(spx['inherit_env'])


if __name__=='__main__':unittest.main(verbosity=2)
