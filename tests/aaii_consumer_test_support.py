"""Exercise actual consumer logic with extreme, stale and forged survey inputs."""
from pathlib import Path
from datetime import datetime
import ast
import copy
import json
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/lambdas/justhodl-aaii-sentiment/tests')]
from storage_aaii_tests import StorageTests, m, s, AT
import aaii_research as adapter


def function(fn, name, namespace):
    path = ROOT/'aws/lambdas'/fn/'source/lambda_function.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace[name]


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        test = StorageTests(); test.setUp()
        self.packet = {**test.output, 'replay': s.retain(test.db, 'test', test.inputs, test.output)}
        self.at = datetime.fromisoformat(AT)

    def test_typed_percentages_and_spread_keep_distinct_units(self):
        doc = adapter.context(self.packet, self.at)
        self.assertTrue(doc['available']); self.assertEqual(doc['bull_bear_spread_pp'], -24.5)
        text = adapter.describe(doc)
        self.assertIn('week ending 2026-09-16', text); self.assertIn('-24.5 percentage points', text)
        self.assertNotIn('contrarian buy', text)

    def test_old_fraction_packet_does_not_gain_native_status(self):
        self.assertFalse(adapter.context({'latest': self.packet['latest']}, self.at)['available'])
        self.assertIsNone(adapter.qualified_signal({'latest': {'bull_bear_spread': -.6}, 'extremes': {'is_bearish_extreme': True}}))

    def test_stale_and_future_clocks_withhold_context(self):
        for at in ('2026-09-22T03:00:00+00:00', '2026-09-20T13:59:59+00:00', '2026-09-25T14:00:00+00:00'):
            self.assertFalse(adapter.context(self.packet, datetime.fromisoformat(at))['available'])

    def test_tampered_body_and_self_qualified_packet_are_rejected(self):
        for key, value in (('as_of', '2026-09-09'), ('calls_eligible', True), ('forecast_qualified', True)):
            packet = copy.deepcopy(self.packet); packet[key] = value
            # Even a caller who recomputes its hash cannot grant model authority.
            if key != 'as_of': packet['replay']['output_sha256'] = m.sha(m.encoded({k:v for k,v in packet.items() if k!='replay'}))
            self.assertFalse(adapter.context(packet, self.at)['available'])
            self.assertIsNone(adapter.qualified_signal(packet))

    def test_signal_board_abstains_even_for_claimed_extreme(self):
        normalizer = function('justhodl-signal-board', 'n_aaii', {})
        for packet in (self.packet, {'latest': {'bull_bear_spread': -.8, 'bearish': .9}, 'calls_eligible': True}):
            with patch.object(adapter, 'context', return_value=adapter.context(self.packet, self.at)):
                vote, text = normalizer(packet)
            self.assertIsNone(vote); self.assertIn('no qualified return forecast or trade vote', text)

    def test_extreme_survey_cannot_add_points_to_every_equity(self):
        packets = [{'latest': {'bull_bear_spread': -.8}, 'extremes': {'is_bearish_extreme': True}}, self.packet]
        for packet in packets:
            ns = {'get_s3_json': lambda key, default=None: packet if 'aaii' in key else {},
                  '_index_by_ticker': lambda _: {}, 'STACKED_CAP_POS': 50, 'STACKED_CAP_NEG': -50}
            load = function('justhodl-asymmetric-scorer', 'load_cross_pollination', ns)
            out = load()
            self.assertEqual(out['broad_market']['aaii_pts'], 0)
            self.assertIsNone(out['broad_market']['aaii_signal'])

    def test_market_extremes_never_compares_fractions_with_eighteen_points(self):
        from extremes_native_test_support import synthesis_with
        out=synthesis_with('aaii',self.packet,self.at,'market-extremes')
        spread=next(r for r in out['measurements'] if r['series_id']=='AAII:bull_bear_spread_pp')
        self.assertEqual(spread['value'],-24.5);self.assertEqual(spread['unit'],'percentage_points')
        self.assertEqual(out['decision']['eligible_votes'],0);self.assertIsNone(out['scores']['top_risk'])

    def test_shared_helper_is_in_every_changed_consumer_package(self):
        sys.path.insert(0, str(ROOT/'aws/ops/checks'))
        from release_package_evidence import shared_imports
        for fn in ('ai-chat','asymmetric-scorer','crisis-knowledge-base','cycle-clock','market-extremes','morning-intelligence','put-call-extreme','signal-board'):
            sources = list((ROOT/'aws/lambdas'/('justhodl-'+fn)/'source').glob('*.py'))
            self.assertIn('aaii_research.py', [p.name for p in shared_imports(ROOT, sources)], fn)

    def test_public_http_reads_only_and_validation_does_not_acquire(self):
        from types import SimpleNamespace
        writes = []
        def forbidden(*args, **kwargs):
            writes.append(args); raise AssertionError('HTTP must not acquire or publish')
        ns = {'json': json, 'CONTRACT': m.CONTRACT, 'CURRENT': s.CURRENT,
              'boto3': SimpleNamespace(client=lambda *args, **kwargs: object()),
              'Config': lambda **kwargs: None, 'run': forbidden,
              'reader': lambda client, bucket: lambda key: m.encoded(self.packet)}
        handler = function('justhodl-aaii-sentiment', 'lambda_handler', ns)
        response = handler({'requestContext': {'http': {'method': 'GET'}}}, None)
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body']), self.packet)
        ns['boto3'] = SimpleNamespace(client=forbidden)
        self.assertEqual(handler({'validate_only': True}, None)['statusCode'], 200)
        self.assertEqual(writes, [])


def run():
    if not unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerTests)).wasSuccessful():
        raise SystemExit(1)


if __name__ == '__main__': run()
