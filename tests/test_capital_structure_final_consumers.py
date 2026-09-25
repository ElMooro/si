from pathlib import Path
from copy import deepcopy
import ast,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
from test_capital_structure_consumers import functions,tree,LEGACY


class Tests(unittest.TestCase):
    def test_whole_predecessor_sources_configs_and_tests_remain_exact(self):
        for name in ('capital-structure-final-consumer-migration','final-consumer-preflight-repair'):
            record=json.loads((ROOT/('tests/fixtures/'+name+'.json')).read_bytes())
            for entry in record['files']:
                raw=(ROOT/entry['predecessor']).read_bytes()
                self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(entry['bytes'],entry['sha256']))

    def test_equity_paid_provider_boundaries_work_without_credentials_sdk_or_network(self):
        scope=functions('justhodl-equity-research',{'claude_call','_anthropic_call'},{'json':json})
        out=json.loads(scope['claude_call']('system','public synthetic input'))
        self.assertEqual(out['verdict']['rating'],'WAIT');self.assertIsNone(out['verdict']['price_target_12m'])
        self.assertFalse(out['sizing_eligible']);self.assertEqual(out['ai_policy'],'no_paid_ai')
        with self.assertRaisesRegex(RuntimeError,'paid_ai_disabled'):scope['_anthropic_call']('system','input')
        source=ast.unparse(tree('justhodl-equity-research'));self.assertNotIn('api.anthropic.com',source)
        self.assertNotIn('ANTHROPIC_KEY',source)

    def test_cached_model_claims_abstain_without_losing_original_fields_or_reported_company_data(self):
        scope=functions('justhodl-equity-research',{'model_research_view','_http_ok'},{'json':json})
        body={'ticker':'ABC','generated_at':'2026-09-24T12:00:00Z','quote':{'price':123},'statements':{'income_annual':[{'revenue':500}]},
            'verdict':{'rating':'BUY','price_target_12m':999,'confidence_pct':99},'forward_model':{'future_return':5},
            'scenarios':{'expected_value_12m':888},'executive_summary':'Old model claim','catalysts_12m':['legacy catalyst']}
        original=deepcopy(body);result=scope['model_research_view'](body)
        self.assertEqual(body,original);self.assertEqual(result['quote'],body['quote']);self.assertEqual(result['statements'],body['statements'])
        self.assertEqual(result['generated_at'],body['generated_at']);self.assertEqual(result['verdict']['rating'],'WAIT')
        self.assertIsNone(result['verdict']['price_target_12m']);self.assertIsNone(result['scenarios']);self.assertIsNone(result['forward_model'])
        self.assertEqual(result['unqualified_model_output']['original_fields']['verdict'],body['verdict'])
        self.assertEqual(result['unqualified_model_output']['original_fields']['scenarios'],body['scenarios'])
        self.assertEqual(scope['model_research_view'](result),result)
        served=json.loads(scope['_http_ok'](body)['body']);self.assertEqual(served,result)
        for key in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):self.assertFalse(served[key])
        self.assertEqual(scope['model_research_view']({'unqualified_model_output':{'original_fields':'invalid'}})['verdict']['rating'],'WAIT')

    def test_buyback_and_equity_share_flows_inputs_are_nonvoting_and_never_restore_legacy_fields(self):
        for fn in ('justhodl-buyback-engine','justhodl-equity-research'):
            nodes=[n for n in ast.walk(tree(fn)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                and n.func.attr=='decision_view' and ast.unparse(n.func.value)=="__import__('capital_structure_context')"]
            self.assertEqual(len(nodes),1)
            scope={'_read':lambda key:LEGACY,'sfd':LEGACY}
            view=eval(compile(ast.Expression(nodes[0]),fn,'eval'),scope)
            self.assertEqual(view['tickers'],{});self.assertFalse(view['calls_eligible'])
        source=ast.unparse(tree('justhodl-equity-research'))
        self.assertIn('cached = model_research_view(json.loads(',source)
        self.assertIn('document = model_research_view(document)',source)

    def test_configs_preserve_runtime_and_do_not_copy_ai_credentials(self):
        for fn,memory,timeout in [('justhodl-buyback-engine',512,600),('justhodl-equity-research',1024,300)]:
            c=json.loads((ROOT/('aws/lambdas/'+fn+'/config.json')).read_bytes())
            self.assertEqual((c['memory'],c['timeout']),(memory,timeout));self.assertFalse(c['inherit_env'])
            self.assertTrue(all('ANTHROPIC' not in k for k in c.get('environment_keys',[])))


if __name__=='__main__':unittest.main(verbosity=2)
