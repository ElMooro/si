from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import Mock, patch
from types import SimpleNamespace
from typing import Optional
import ast, importlib.util, json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests')]
import massive_research_context as gate
import massive_signals as accessor
from test_inflection_authority import functions
LEGACY = {'tickers': {'SPY': {'gamma_squeeze_score': 100, 'prepump_score': 100, 'bullish_flow': True, 'otm_call_sweep': True}},
    'top_prepump': [{'symbol': 'SPY', 'prepump_score': 100}], 'market': {'gamma_regime': 'NEGATIVE_GAMMA', 'smallcap_bid': True}}


def native():
    return {'contract': 'massive-composite-research.v1', 'generated_at': '2020-01-01T00:00:00Z',
        'replay': {'manifest_key': 'data/massive-research/runs/'+'a'*64+'.json', 'output_sha256': 'b'*64},
        **{k: False for k in gate.FLAGS}, **LEGACY}


class BoundaryTests(unittest.TestCase):
    def test_legacy_and_self_proclaimed_native_scores_never_become_votes(self):
        for packet in (LEGACY, native(), {**native(), 'calls_eligible': True}, None):
            out = gate.context(packet)
            self.assertEqual(out['tickers'], {}); self.assertEqual(out['market'], {}); self.assertEqual(out['top_prepump'], [])
            self.assertEqual(out['independent_investment_votes'], 0); self.assertFalse(out['calls_eligible'])
    def test_reference_does_not_claim_independent_hash_verification(self):
        out = gate.context(native()); self.assertTrue(out['native_reference_available'])
        self.assertFalse(out['reference_hashes_independently_checked_by_consumer'])
    def test_v2_native_and_compatibility_reference_preserve_abstention(self):
        packet={**native(), 'contract':'massive-composite-research.v2'}
        alias={'contract':'massive-research-compatibility.v2','generated_at':packet['generated_at'],
            'canonical':{'key':gate.CURRENT,'replay':packet['replay']},'tickers':{},'top_prepump':[],'market':{},
            **{k:False for k in gate.FLAGS}}
        for value in (packet,alias):
            out=gate.context(value);self.assertTrue(out['native_reference_available'])
            self.assertEqual(out['tickers'],{});self.assertEqual(out['independent_investment_votes'],0)
            self.assertFalse(out['reference_hashes_independently_checked_by_consumer'])
    def test_bad_clock_and_wrong_namespace_cannot_be_native_reference(self):
        for stamp in (None, '2020-01-01', '9999-01-01T00:00:00Z'):
            self.assertFalse(gate.context({**native(), 'generated_at': stamp})['native_reference_available'])
        p = native(); p['replay']['manifest_key'] = 'data/trade-tickets.json'
        self.assertFalse(gate.context(p)['native_reference_available'])
    def test_unrelated_engine_is_unchanged(self):
        self.assertIs(gate.guard('data/other-research.json', LEGACY), LEGACY)
    def test_convergence_direct_extractor_cannot_bypass_fetch_guard(self):
        scope = functions('convergence-radar', {'extract_ticker_signals_from_engine'}, {})
        for name, spec in (('massive-flow', {}), ('renamed', {'key': gate.LEGACY})):
            self.assertEqual(scope['extract_ticker_signals_from_engine'](name, spec, LEGACY['top_prepump']), {})
    def test_convergence_fetch_rejects_old_raw_ranks(self):
        s3 = Mock(); s3.get_object.return_value = {'Body': BytesIO(json.dumps(LEGACY).encode()), 'LastModified': datetime.now(timezone.utc)}
        scope = functions('convergence-radar', {'fetch_engine_raw'}, {'s3': s3, 'S3_BUCKET': 'synthetic', 'json': json, 'datetime': datetime, 'timezone': timezone})
        self.assertEqual(scope['fetch_engine_raw']('massive-flow', {'key': gate.LEGACY, 'path': 'top_prepump'})[1], [])
    def test_actual_best_setups_master_ranker_and_pump_reads_are_guarded(self):
        # Execute the real source expressions with a malicious legacy packet;
        # avoid invoking the surrounding handlers, their providers or alerts.
        for fn, target in (('best-setups', 'massive'), ('pump-mechanics', '_massive_t')):
            tree = ast.parse((ROOT/f'aws/lambdas/justhodl-{fn}/source/lambda_function.py').read_text(encoding='utf-8'))
            assignment = next(n for n in ast.walk(tree) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == target for t in n.targets))
            s3 = Mock(); s3.get_object.return_value = {'Body': BytesIO(json.dumps(LEGACY).encode())}
            scope = {'read_json': lambda key: LEGACY, 's3': s3, 'S3_BUCKET': 'synthetic', 'json': json}
            exec(compile(ast.Module(body=[assignment], type_ignores=[]), fn, 'exec'), scope)
            self.assertEqual(scope[target]['tickers'] if fn == 'best-setups' else scope[target], {})
        tree = ast.parse((ROOT/'aws/lambdas/justhodl-master-ranker/source/lambda_function.py').read_text(encoding='utf-8'))
        item = next(v for n in ast.walk(tree) if isinstance(n, ast.Dict) for k, v in zip(n.keys, n.values)
            if isinstance(k, ast.Constant) and k.value == 'massive' and isinstance(v, ast.Call))
        out = eval(compile(ast.Expression(item), 'master-ranker actual feed', 'eval'), {'fetch_json': lambda *a, **kw: LEGACY})
        self.assertEqual(out['tickers'], {})
    def test_alpha_uses_bounded_accessor_not_indefinite_cache(self):
        scope = functions('alpha-score', {'_massive_tickers'}, {})
        with patch.object(accessor, 'massive_research', return_value=gate.context(LEGACY)):
            self.assertEqual(scope['_massive_tickers'](), {})


class CacheTests(unittest.TestCase):
    def setUp(self): accessor._CACHE.clear(); self.addCleanup(accessor._CACHE.clear)
    def test_refresh_expiration_and_returned_mutation_isolation(self):
        client = Mock(); client.get_object.side_effect = lambda **kw: {'Body': BytesIO(json.dumps(native()).encode())}
        with patch.object(accessor.boto3, 'client', return_value=client), patch.object(accessor.time, 'monotonic', return_value=10):
            first = accessor.massive_research(); first['canonical']['key'] = 'tampered'
            self.assertEqual(accessor.massive_research()['canonical']['key'], gate.CURRENT); self.assertEqual(client.get_object.call_count, 1)
        with patch.object(accessor.boto3, 'client', return_value=client), patch.object(accessor.time, 'monotonic', return_value=40):
            accessor.massive_research(); self.assertEqual(client.get_object.call_count, 2)
    def test_failed_fetch_recovers_after_five_seconds(self):
        client = Mock(); client.get_object.side_effect = RuntimeError('unavailable')
        with patch.object(accessor.boto3, 'client', return_value=client), patch.object(accessor.time, 'monotonic', return_value=10):
            self.assertFalse(accessor.massive_research()['native_reference_available']); accessor.massive_research()
            self.assertEqual(client.get_object.call_count, 1)
        client.get_object.side_effect = lambda **kw: {'Body': BytesIO(json.dumps(native()).encode())}
        with patch.object(accessor.boto3, 'client', return_value=client), patch.object(accessor.time, 'monotonic', return_value=15):
            self.assertTrue(accessor.massive_research()['native_reference_available'])
            self.assertEqual(client.get_object.call_count, 2)
    def test_failed_refresh_discards_previously_available_reference(self):
        client = Mock(); client.get_object.return_value = {'Body': BytesIO(json.dumps(native()).encode())}
        with patch.object(accessor.boto3, 'client', return_value=client), patch.object(accessor.time, 'monotonic', return_value=10):
            self.assertTrue(accessor.massive_research()['native_reference_available'])
        client.get_object.side_effect = RuntimeError('unavailable')
        with patch.object(accessor.boto3, 'client', return_value=client), patch.object(accessor.time, 'monotonic', return_value=40):
            self.assertIsNone(accessor.massive_research()['canonical'])
    def test_theme_and_legacy_interfaces_cannot_emit_scores(self):
        with patch.object(accessor, '_load', return_value=gate.context(native())):
            self.assertEqual(accessor.massive_market(), {}); self.assertEqual(accessor.massive_ticker('SPY'), {})
            self.assertEqual(accessor.massive_prepump(), []); self.assertIsNone(accessor.sector_flow_z('XLK'))


class HandlerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('massive_native_handler', ROOT/'aws/lambdas/justhodl-massive-signals/source/lambda_function.py')
        cls.m = importlib.util.module_from_spec(spec); spec.loader.exec_module(cls.m)
    def test_validation_never_constructs_a_client(self):
        with patch.object(self.m.boto3, 'client', side_effect=AssertionError('No AWS')):
            self.assertEqual(self.m.lambda_handler({'validate_only': True})['statusCode'], 200)
    def test_http_is_read_only_and_never_falls_back(self):
        client = Mock(); client.get_object.return_value = {'Body': BytesIO(b'{}')}
        with patch.object(self.m.boto3, 'client', return_value=client), patch.object(self.m, 'run') as run:
            self.assertEqual(self.m.lambda_handler({'httpMethod': 'GET'})['statusCode'], 503); run.assert_not_called()
            self.assertEqual(client.get_object.call_args.kwargs['Key'], gate.CURRENT)
    def test_native_http_reads_without_recomputation(self):
        packet = native(); client = Mock(); client.get_object.return_value = {'Body': BytesIO(json.dumps(packet).encode())}
        with patch.object(self.m.boto3, 'client', return_value=client), patch.object(self.m, 'run') as run:
            self.assertEqual(json.loads(self.m.lambda_handler({'action': 'current_state'})['body']), packet); run.assert_not_called()
    def test_recovery_keeps_request_and_execution_identity(self):
        with patch.object(self.m.boto3, 'client'), patch.object(self.m, 'run', return_value={'status': 'complete'}) as run:
            self.m.lambda_handler({'request_id': 'durable', 'recover_run': {'manifest_key': 'qualified'}}, SimpleNamespace(aws_request_id='execution'))
            self.assertEqual(run.call_args.args[2:], ('durable', 'execution'))
            self.assertEqual(run.call_args.kwargs['recover_run'], {'manifest_key': 'qualified'})
    def test_no_execution_without_aws_identity(self):
        with patch.object(self.m.boto3, 'client'), patch.object(self.m, 'run') as run:
            with self.assertRaises(ValueError): self.m.lambda_handler({})
            run.assert_not_called()
    def test_publication_callback_rejects_wrong_path(self):
        with self.assertRaises(ValueError): self.m.publish_current(None, 'b', 'data/trade-tickets.json', b'{}', {'IfMatch': 'x'})


if __name__ == '__main__': unittest.main()
