from pathlib import Path
from unittest.mock import Mock
from io import BytesIO
import ast,copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
import offexchange_context as gate
from release_package_evidence import shared_imports
LEGACY={'generated_at':'2026-09-24T20:00:00Z','board':[{'ticker':'AAPL','state':'ACCUMULATION','score':99}],
        'dark_map':{'AAPL':999999},'xray_map':{'AAPL':{'score':99}},'distribution':{'accumulation':999},'dix':{'own_dix_pct':99}}
def native():return json.loads(json.loads((ROOT/'tests/fixtures/offexchange-native.json').read_text(encoding='utf-8'))['objects'][gate.CURRENT])
def source(name):return (ROOT/f'aws/lambdas/justhodl-{name}/source/lambda_function.py').read_text(encoding='utf-8')
def functions(name,wanted,scope=None):
    scope={} if scope is None else scope;nodes=[n for n in ast.parse(source(name)).body if isinstance(n,ast.FunctionDef) and n.name in wanted]
    assert len(nodes)==len(wanted);exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope);return scope
class Tests(unittest.TestCase):
    def test_native_legacy_promoted_and_missing_inputs_never_supply_votes(self):
        for p in (None,{},LEGACY,native(),{**native(),'calls_eligible':True}):
            out=gate.decision_view(p);self.assertEqual(out['board'],[]);self.assertEqual(out['dark_map'],{});self.assertIsNone(out['dix']['own_dix_pct']);self.assertTrue(all(out[k] is False for k in gate.FLAGS))
        context=gate.context(native());self.assertTrue(context['native_reference_available']);self.assertTrue(context['output_digest_checked']);self.assertFalse(context['original_provider_replay_performed_by_consumer']);self.assertFalse(context['current_freshness_verified_by_consumer'])
        self.assertFalse(gate.context({**native(),'changed':1})['native_reference_available'])
    def test_unrelated_key_is_returned_unchanged(self):
        self.assertIs(gate.guard('data/other.json',LEGACY),LEGACY)
    def test_every_reviewed_literal_read_expression_is_guarded_and_bundled(self):
        manifest=json.loads((ROOT/'tests/fixtures/offexchange-consumer-migration.json').read_text(encoding='utf-8'))
        for name,entry in manifest['consumers'].items():
            paths=list((ROOT/f'aws/lambdas/justhodl-{name}/source').glob('*.py'));self.assertIn('offexchange_context.py',[p.name for p in shared_imports(ROOT,paths)],name)
            if entry['method']!='wrapped_public_read':continue
            tree=ast.parse(source(name));count=0
            for node in ast.walk(tree):
                if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr=='decision_view' and ast.unparse(node.func.value)=='__import__(\'offexchange_context\')':
                    scope={entry['reader']:lambda *args:copy.deepcopy(LEGACY)}
                    got=eval(compile(ast.Expression(body=node),name,'eval'),scope);self.assertEqual(got['board'],[]);count+=1
            self.assertEqual(count,entry['count'],name)
    def test_actual_key_readers_preserve_other_feeds_and_block_legacy_direction(self):
        for name,fn in [('accum-composite','read_json'),('distribution-composite','read_json'),('equity-confluence','_read'),('institutional-footprint','_j')]:
            client=Mock();client.get_object.side_effect=lambda **kw:{'Body':BytesIO(json.dumps(LEGACY).encode())}
            scope=functions(name,{fn},{'json':json,'s3':client,'BUCKET':'b'});self.assertEqual(scope[fn](gate.CURRENT)['board'],[]);self.assertEqual(scope[fn]('data/untouched.json'),LEGACY)
    def test_ignition_never_restores_direct_provider_fallback(self):
        for p in (LEGACY,native(),{}):
            client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps(p).encode())}
            network=Mock(side_effect=AssertionError('No provider'))
            scope=functions('ignition',{'load_dark'},{'json':json,'S3':client,'BUCKET':'b','http_json':network});self.assertIsNone(scope['load_dark']());network.assert_not_called()
        scope=functions('signal-board',{'n_darkpool'})
        for p in (LEGACY,native(),{}):self.assertIsNone(scope['n_darkpool'](p)[0])
    def test_shared_adapter_has_no_invented_direction_or_confidence(self):
        from jh_adapters import DarkPoolAdapter
        adapter=object.__new__(DarkPoolAdapter);adapter.engine_id='dark_pool'
        for p in (LEGACY,native(),{}):
            rows=list(adapter.rows(p));self.assertEqual(len(rows),1);self.assertIn('skip',rows[0]);self.assertNotIn('confidence',rows[0])
            result=adapter.parse_existing_output(p,{});self.assertEqual(result.source_status,'UNQUALIFIED');self.assertEqual(result.signals,[])
    def test_stored_composites_cannot_smuggle_legacy_finra_components(self):
        from holdings_derived_boundary import BASIS,flow_rows,flow_annotations
        from capital_research_boundary import context
        row={'ticker':'AAPL','engines':['dark-pool','options-flow'],'n_engines':2,'score':1.05,
             'posture':'ACCUMULATION','tags':['legacy'],'heavy_short':False,'stealth':False}
        p={'holdings_exclusions':{'basis':BASIS},'capital_flow_exclusion':context({}),
           'multi_engine_confluence':[row],'ticker_map':{'AAPL':row}}
        self.assertEqual(flow_rows(p),[]);self.assertEqual(flow_annotations(p),{})
if __name__=='__main__':unittest.main(verbosity=2)
