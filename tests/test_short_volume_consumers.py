from pathlib import Path
from unittest.mock import Mock
from datetime import datetime, timezone
from io import BytesIO
import ast, copy, hashlib, json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'aws/ops/checks'), str(ROOT / 'tests')]
from release_package_evidence import shared_imports
import short_volume_context as gate
from test_short_volume_context import packet

LEGACY = {'generated_at': '2026-09-24T20:00:00Z', 'n_pressure_building': 0, 'n_shorts_covering': 50,
          'names': [{'ticker': 'AAPL', 'state': 'SHORTS COVERING', 'z_score': -4}],
          'tickers': {'AAPL': {'svr_pct': 90, 'days_to_cover': 30, 'squeeze_score': 99}},
          'squeeze_candidates': [{'symbol': 'AAPL', 'squeeze_score': 99}], 'market_composite': {'regime': 'HIGH_SHORT_PRESSURE'}}


def source(name):
    return (ROOT / f'aws/lambdas/justhodl-{name}/source/lambda_function.py').read_text(encoding='utf-8')


def functions(name, wanted, scope=None):
    scope = {} if scope is None else scope
    nodes = [n for n in ast.parse(source(name)).body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    assert len(nodes) == len(wanted)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), name, 'exec'), scope)
    return scope


class Tests(unittest.TestCase):
    def test_every_complete_predecessor_is_retained_with_exact_hash_and_byte_count(self):
        manifest = json.loads((ROOT / 'tests/fixtures/short-volume-consumer-migration.json').read_bytes())
        for entry in manifest['archives'].values():
            raw = (ROOT / entry['file']).read_bytes()
            self.assertEqual(len(raw), entry['bytes'])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), entry['sha256'])

    def test_reviewed_read_expressions_are_actually_guarded_and_helpers_are_packaged(self):
        manifest = json.loads((ROOT / 'tests/fixtures/short-volume-consumer-migration.json').read_bytes())
        for name, entry in manifest['consumers'].items():
            paths = list((ROOT / f'aws/lambdas/justhodl-{name}/source').glob('*.py'))
            self.assertIn('short_volume_context.py', [p.name for p in shared_imports(ROOT, paths)], name)
            if entry['method'] not in ('wrapped_public_read', 'wrapped_symbolic_read'):
                continue
            count = 0
            for node in ast.walk(ast.parse(source(name))):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'decision_view'
                        and ast.unparse(node.func.value) == "__import__('short_volume_context')"):
                    reader = entry['reader']
                    if reader == 'fleet_io.get_json':
                        scope = {'fleet_io': Mock()}
                        scope['fleet_io'].get_json.return_value = copy.deepcopy(LEGACY)
                    else:
                        scope = {reader: lambda *args, **kwargs: copy.deepcopy(LEGACY)}
                    scope.update(s3=None, FINRA_SHORT_KEY=gate.CURRENT, FEED_FINRA=gate.CURRENT)
                    actual = eval(compile(ast.Expression(body=node), name, 'eval'), scope)
                    self.assertEqual(actual['names'], [], name)
                    self.assertEqual(actual['tickers'], {}, name)
                    self.assertEqual(actual['squeeze_candidates'], [], name)
                    count += 1
            self.assertEqual(count, entry['count'], name)

    def test_key_readers_leave_unrelated_feeds_unchanged(self):
        for name, fn in [('accum-composite', 'read_json'), ('distribution-composite', 'read_json'), ('stealth-accumulation', 'read_s3')]:
            client = Mock()
            client.get_object.side_effect = lambda **kw: {'Body': BytesIO(json.dumps(LEGACY).encode())}
            scope = functions(name, {fn}, {'json': json, 's3': client, 'BUCKET': 'b', 'S3_BUCKET': 'b'})
            def call(key):
                return scope[fn](client, key) if fn == 'read_s3' else scope[fn](key)
            self.assertEqual(call(gate.CURRENT)['tickers'], {}, name)
            self.assertEqual(call(gate.ALIAS)['names'], [], name)
            self.assertEqual(call('data/short-interest.json'), LEGACY, name)

    def test_signal_and_conviction_normalizers_abstain_instead_of_neutral_zero(self):
        for name in ('signal-board', 'conviction-engine'):
            fn = functions(name, {'n_short_pressure'})['n_short_pressure']
            for value in (LEGACY, packet(), {}, None):
                self.assertIsNone(fn(value)[0])

    def test_regime_reader_has_no_daily_short_interest_or_direction_vote(self):
        for value in (LEGACY, packet()):
            client = Mock()
            client.get_object.return_value = {'Body': BytesIO(json.dumps(value).encode())}
            scope = functions('regime-composite', {'fetch_module'}, {'S3': client, 'BUCKET': 'b', 'json': json,
                               'datetime': datetime, 'timezone': timezone, 'CISS_SERIES': {}})
            out = scope['fetch_module']({'key': gate.CURRENT, 'label': 'FINRA Short-Sale Volume', 'emoji': '', 'dimension': 'smart_money'})
            self.assertIsNone(out['polarity'])
            self.assertFalse(out['vote_eligible'])
            self.assertEqual(out['evidence_family'], 'finra_reported_equity_activity')

    def test_raw_s3_readers_cannot_restore_stored_covering_or_squeeze_fields(self):
        for name, fn in [('risk-radar', 'load_short_pressure'), ('squeeze-fuel', 'fetch_daily_shortvol')]:
            client = Mock()
            client.get_object.return_value = {'Body': BytesIO(json.dumps(LEGACY).encode())}
            scope = functions(name, {fn}, {'S3': client, 's3': client, 'BUCKET': 'b', 'S3_BUCKET': 'b', 'json': json})
            self.assertEqual(scope[fn](), {})

    def test_daily_volume_cannot_become_low_lending_risk_via_empty_default(self):
        fn = functions('repo-lending', {'compute_utilization_score'})['compute_utilization_score']
        self.assertIsNone(fn(gate.decision_view(LEGACY))[0])
        self.assertEqual(fn({'items': [{'ticker': 'ABC', 'utilization': 90}]})[0], 25)

    def test_old_stored_flow_components_are_rejected_without_affecting_unrelated_components(self):
        from holdings_derived_boundary import BASIS, flow_rows, flow_annotations
        from capital_research_boundary import context
        row = {'ticker': 'AAPL', 'engines': ['finra-short', 'options-flow'], 'n_engines': 2, 'score': 1.05,
               'posture': 'SHORT_SQUEEZE_SETUP', 'tags': ['legacy'], 'heavy_short': True, 'stealth': False}
        value = {'holdings_exclusions': {'basis': BASIS}, 'capital_flow_exclusion': context({}),
                 'multi_engine_confluence': [row], 'ticker_map': {'AAPL': row}}
        self.assertEqual(flow_rows(value), [])
        self.assertEqual(flow_annotations(value), {})
        row['engines'] = ['insider', 'options-flow']
        self.assertEqual(flow_rows(value), [row])
        self.assertIn('AAPL', flow_annotations(value))


if __name__ == '__main__':
    unittest.main(verbosity=2)
