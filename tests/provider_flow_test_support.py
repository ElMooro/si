"""Actual public handlers/consumer reads with hostile legacy data; no cloud calls."""
from pathlib import Path
from datetime import datetime, timezone
from unittest import mock
from typing import Optional
import ast, copy, gzip, io, json, os, sys, time, types, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'aws/shared/tests')]
import provider_flow_research as boundary
import provider_flow_model as model
import provider_flow_store as store
from test_provider_flow_store import Storage, source_fixture

CANARY = {'generated_at': '2026-09-21T06:00:00Z', 'calls_eligible': True,
    'metrics': [{'ticker': 'SPY', 'flow_zscore_90d': 99, 'flow_5d_usd': 1e12,
                 'quadrant': 'STEALTH_ACCUMULATION', 'divergence_score': 99}],
    'SPY': {'score': 99}, 'context': {'by_sector': {'Technology': {'prompt_snippet': 'BUY NOW'}}},
    'events': [{'ticker': 'SPY', 'quadrant': 'STEALTH', 'date': '2099-01-01'}],
    'top_constituents_by_pressure': [{'stock': 'SPY', 'total_pressure_5d_usd': 1e12}]}
READERS = {
    'ai-rerating-radar': ['_read'], 'apac-flows': ['_j'], 'bond-desk': ['_s3json'],
    'bottom': ['s3_json'], 'flow-anomaly-detector': ['_read_json'], 'equity-confluence': ['_read'],
    'fortress': ['s3_json'], 'industry-rotation': ['s3_json'], 'katlin': ['s3_json', 's3_json_quiet'],
    'macro-confluence': ['_get'], 'theme-cascade': ['_read_json'], 'theme-cascade-backtest': ['_read_json'],
    'theme-rotation': ['_read_s3_json'], 'sector-emergence': ['_read'],
    'best-ideas': ['_load_json'], 'boom-radar': ['getj'],
    'impact-graph': ['_get_json'], 'flow-confluence': ['_read'], 'flows-ai-analysis': ['_read_json']}


def actual(short, name, ns):
    p = ROOT / 'aws/lambdas' / ('justhodl-' + short) / 'source/lambda_function.py'
    tree = ast.parse(p.read_text(encoding='utf-8'))
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(p), 'exec'), ns)
    return ns[name]


class Client:
    def __init__(self, packet=CANARY): self.packet = packet; self.reads = []; self.writes = {}
    def get_object(self, **kw):
        self.reads.append(kw['Key'])
        return {'Body': io.BytesIO(json.dumps(self.packet).encode()), 'LastModified': datetime.now(timezone.utc)}
    def put_object(self, **kw): self.writes[kw['Key']] = json.loads(kw['Body'])


class NativeHandlers(unittest.TestCase):
    def test_actual_native_validation_http_and_scheduled_dispatch_are_separate(self):
        for kind, short, contract, key in (('flow', 'etf-fund-flows', model.CONTRACT, model.CURRENT),
                ('radar', 'capital-flow-radar', model.RADAR_CONTRACT, model.RADAR_CURRENT)):
            client = Client(); creator = mock.Mock(return_value=client)
            runner = mock.Mock(return_value={'status': 'complete', 'published': True})
            ns = {'json': json, 'os': os, 'boto3': types.SimpleNamespace(client=creator),
                  'Config': lambda **kw: kw, 'CONTRACT': contract, 'KIND': kind,
                  'PUBLISHED_KEY': key, 'reader': lambda *a: lambda k: json.dumps(client.packet).encode(), 'run': runner}
            ns['publish_current'] = actual(short, 'publish_current', ns)
            f = actual(short, 'lambda_handler', ns)
            self.assertEqual(f({'validate_only': True}, None)['statusCode'], 200); creator.assert_not_called()
            self.assertEqual(f({'action': 'current_state'}, None)['statusCode'], 503); runner.assert_not_called()
            client.packet = {'contract': contract, 'call': None}
            self.assertEqual(json.loads(f({'httpMethod': 'GET'}, None)['body']), client.packet); runner.assert_not_called()
            with self.assertRaises(ValueError): f({}, None)
            ctx = types.SimpleNamespace(aws_request_id='execution', get_remaining_time_in_millis=lambda: 287000)
            with mock.patch.dict(os.environ, {'POLYGON_KEY': 'synthetic-local-only'}):
                self.assertEqual(f({'request_id': 'retained-once'}, ctx)['statusCode'], 200)
            self.assertEqual(runner.call_args.args[2:5], (kind, 'retained-once', 'execution'))
            self.assertEqual(runner.call_args.kwargs['remaining_seconds'], 287)
            self.assertEqual(runner.call_args.kwargs['credential'], 'synthetic-local-only' if kind == 'flow' else '')
            db = Storage(); candidate = {'contract': contract, 'generated_at': '2026-09-21T06:00:00Z'}
            self.assertTrue(store.conditional(db, 'b', key, candidate, ns['publish_current']))
            self.assertEqual(json.loads(db.objects[key]), candidate)
            with self.assertRaises(ValueError): ns['publish_current'](db, 'b', 'private/unreviewed.json', b'{}', {'IfNoneMatch': '*'})
            with self.assertRaises(ValueError): ns['publish_current'](db, 'b', key, b'{}', {})

    def test_compatibility_paths_link_complete_replay_and_never_restore_legacy_scores(self):
        s = Storage(); inputs = source_fixture(s); out, histories = store.compile_output(inputs, store.reader(s, 'b'))
        ref = store.retain(s, 'b', inputs, out, histories); packet = {**out, 'replay': ref}
        self.assertTrue(all(store.publish_aliases(s, 'b', packet).values()))
        self.assertEqual(len(store.ALIASES), 14)
        for key in store.ALIASES:
            view = json.loads(s.objects[key]); self.assertEqual(view, model.compatibility(packet, key))
            self.assertEqual(len(view['inventory']), 300); self.assertEqual(view['metrics'], [])
            self.assertEqual(view['canonical']['replay'], ref)
            self.assertEqual(boundary.guard(key, view), {})
            self.assertEqual(view['retained_predecessor'], inputs['contexts'][key])
        old = copy.deepcopy(packet); old['generated_at'] = '2026-09-20T00:00:00Z'
        self.assertFalse(any(store.publish_aliases(s, 'b', old).values()))


class FlowBoundaries(unittest.TestCase):
    def test_native_desk_requires_original_canonical_evidence_instead_of_legacy_scores(self):
        import etf_desk_store as desk
        from test_etf_desk_store import fixture
        with mock.patch.object(desk.catalog,'DESK',('SPY','VOO','BND')),mock.patch.dict(desk.flow_catalog.ETF_UNIVERSE,
                {'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True):
            db,inputs=fixture();read=desk.reader(db,'fixture')
            inputs['canonical_flows']={'source_key':desk.flow_model.CURRENT,
                **desk.protect(db,'fixture',desk.model.encoded(CANARY),read)}
            with self.assertRaises(ValueError):desk.compile_output(inputs,read,lambda *a:self.fail('No artifact from unqualified source'))

    def test_actual_consumer_readers_exclude_the_family_and_preserve_unrelated_data(self):
        for short, names in READERS.items():
            for name in names:
                client = Client()
                ns = dict(json=json, s3=client, S3=client, S3_BUCKET='b', BUCKET='b',
                          gzip=gzip, Optional=Optional, log=lambda *a: None)
                f = actual(short, name, ns)
                for key in ('etf-flows/daily.json', 'etf-flows/stock-exposure-lookup.json',
                            'etf-flows/history/2026-09-20.json', 'data/flow-lookthrough.json'):
                    self.assertEqual(f(key), {}, (short, name, key))
                self.assertEqual(f('data/unrelated.json'), CANARY, (short, name))

    def test_fixed_key_reads_cannot_forward_legacy_narratives_or_scores(self):
        for short, names in {'analytics-snapshot': ['flow_doc'], 'equity-research': ['ctx', 'lookup'],
                'research-critique': ['ctx', 'press_doc'], 'index-recon': ['_fl']}.items():
            p = ROOT / 'aws/lambdas' / ('justhodl-' + short) / 'source/lambda_function.py'
            tree = ast.parse(p.read_text(encoding='utf-8'))
            for name in names:
                node = next(n for n in ast.walk(tree) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)
                    and 'provider_flow_research' in ast.unparse(n))
                client = Client(); ns = {'json': json, 's3': client, 'S3_BUCKET': 'b',
                    'obj': client.get_object(Key='unused'), 'ETF_FLOWS_KEY': 'etf-flows/daily.json'}
                exec(compile(ast.Module(body=[node], type_ignores=[]), str(p), 'exec'), ns)
                self.assertEqual(ns[name], {})

    def test_actual_ranker_and_desk_abstain_without_counting_a_neutral_vote(self):
        health = []; f = actual('master-ranker', 'fetch_json', {'_FEED_HEALTH': health})
        self.assertIsNone(f('data/flow-lookthrough.json')); self.assertFalse(health[0]['used'])
        client = Client(); f = actual('quantum-desk', 'read_source',
            dict(LOCAL_DIR=None, s3=client, BUCKET='b', json=json, _now=lambda: datetime.now(timezone.utc)))
        doc, meta = f('etf_flows', {'key': 'etf-flows/daily.json', 'max_age_h': 40})
        self.assertIsNone(doc); self.assertEqual(meta['status'], 'research_only')

    def test_actual_legacy_ai_and_stealth_cannot_make_paid_calls_or_emit_signals(self):
        ai = actual('flows-ai-analysis', 'lambda_handler', dict(time=time, datetime=datetime, timezone=timezone, json=json))
        result = json.loads(ai({}, None)['body']); self.assertEqual(result['paid_ai_calls'], 0)
        self.assertEqual(result['notifications_sent'], 0); self.assertFalse(result['calls_eligible'])
        def forbidden(*a, **kw): self.fail('No signal, price lookup or account access authorized')
        client = Client()
        ns = dict(time=time, datetime=datetime, timezone=timezone, timedelta=__import__('datetime').timedelta,
            json=json, s3=client, S3_BUCKET='b', SRC_KEY='data/etf-flows/event-study.json',
            OUT_KEY='data/stealth-flow.json', VERSION='test',
            ddb=types.SimpleNamespace(Table=lambda _: None), log_signal=forbidden, yprice=forbidden)
        actual('stealth-flow', 'lambda_handler', ns)({}, None)
        packet = client.writes['data/stealth-flow.json']
        self.assertEqual(packet['recent_stealth'], []); self.assertEqual(packet['logged'], 0)
        self.assertFalse(packet['calls_eligible']); self.assertNotIn('75%', packet['methodology'])

    def test_inventory_is_complete_and_missing_stock_flow_is_not_zero(self):
        rows = actual('etf-census', 'flow_records', {})()
        self.assertEqual(len(rows), 300); self.assertNotIn('flow_5d_usd', rows['SPY'])
        holdings = {'SPY': {'processed_date': '2026-09-18', 'top_constituents': [
            {'stock': 'AAPL', 'weight_pct': 5}, {'stock': 'AAPL', 'weight_pct': None}]}}
        out = boundary.holdings_inventory([rows['SPY']], holdings)['AAPL']
        self.assertEqual(out['n_etfs_holding'], 1); self.assertEqual(len(out['holding_etfs']), 2)
        self.assertIsNone(out['total_aggregate_flow_5d_usd']); self.assertIsNone(out['cumulative_weight_pct'])
        self.assertIsNone(out['quadrant']); self.assertFalse(out['calls_eligible'])

    def test_existing_composite_cannot_smuggle_provider_flow_components(self):
        import holdings_derived_boundary as h
        for component in ('etf-lookthrough', 'stealth'):
            row = {'engines': ['dark-pool', component], 'n_engines': 2}
            self.assertFalse(h._flow_components(row))
        self.assertTrue(h._flow_components({'engines': ['insider'], 'n_engines': 1}))
        self.assertFalse(h._flow_components({'engines': ['dark-pool'], 'n_engines': 1}))

    def test_actual_native_holdings_handlers_keep_validation_http_and_collection_separate(self):
        from etf_holdings_test_support import check_handlers
        check_handlers(self)


if __name__ == '__main__': unittest.main(verbosity=2)
