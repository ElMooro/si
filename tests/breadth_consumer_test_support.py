"""Offline tests of the shared public series route used by four research consumers."""
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
import hashlib, io, json, sys, unittest, ast
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import series_source as ss


def packet():
    at = datetime.now(timezone.utc); day = at.date().isoformat()
    doc = {'contract': 'breadth-native-research.v1', 'generated_at': at.isoformat(), 'as_of': day,
        'calendar': {'requested_sessions': ['2026-01-02', '2026-01-05', day]},
        'series': {'ADVANCERS': {'2026-01-02': 10, '2026-01-05': None, day: 0}}}
    raw = json.dumps(doc, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    return {**doc, 'replay': {'output_sha256': hashlib.sha256(raw).hexdigest()}}


class ConsumerTests(unittest.TestCase):
    def setUp(self): ss._INTERNALS_CACHE = {}; ss._INTERNALS_CACHE_AT = 0

    def test_public_fetch_does_not_bridge_a_gap_or_replace_zero(self):
        import boto3
        class Client:
            def get_object(self, **kw):
                assert kw['Key'] == 'data/market-internals.json'
                return {'Body': io.BytesIO(json.dumps(packet()).encode())}
        with patch.object(boto3, 'client', return_value=Client()):
            data = ss.fetch('INTERNALS', 'ADVANCERS')
        self.assertEqual(data, {datetime.now(timezone.utc).date().isoformat(): 0.0})

    def test_container_cache_retries_after_its_bound(self):
        import boto3
        class Client:
            def __init__(self): self.calls = 0
            def get_object(self, **kw):
                self.calls += 1
                if self.calls == 1: raise ValueError('synthetic unavailable source')
                return {'Body': io.BytesIO(json.dumps(packet()).encode())}
        client = Client()
        with patch.object(boto3, 'client', return_value=client), patch.object(ss._time, 'monotonic', side_effect=[100,100,200,401,401]):
            self.assertEqual(ss._internals('ADVANCERS','2020-01-01'), {})
            self.assertEqual(ss._internals('ADVANCERS','2020-01-01'), {})
            self.assertTrue(ss._internals('ADVANCERS','2020-01-01'))
        self.assertEqual(client.calls, 2)

    def test_native_identity_is_explicit_not_a_vendor_replacement(self):
        for symbol in ('USI:ADVN.NY','USI:ADVQ','USI:TICK','USI:TRINQ','USI:MCCL','USI:ACTV','USI:BASPRD'):
            self.assertEqual(ss.map_symbol(symbol)[:3], (None, None, 0))
        self.assertEqual(ss.map_symbol('JH_BREADTH:ADVANCERS')[:2], ('INTERNALS', 'ADVANCERS'))

    def test_signal_board_abstains_even_when_a_producer_claims_authority(self):
        tree = ast.parse((ROOT/'aws/lambdas/justhodl-signal-board/source/lambda_function.py').read_text(encoding='utf-8'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'n_market_internals')
        scope = {}; exec(compile(ast.Module(body=[node], type_ignores=[]), '<reviewed normalizer>', 'exec'), scope)
        for p in ({}, {'calls_eligible': True, 'mcclellan': {'oscillator': -200}, 'zweig_thrust': {'fired': True}}):
            self.assertIsNone(scope['n_market_internals'](p)[0])


def run(function):
    tree = ast.parse((ROOT/'aws/lambdas'/function/'source/lambda_function.py').read_text(encoding='utf-8'))
    assert any(isinstance(n, ast.Import) and any(a.name == 'series_source' for a in n.names) for n in ast.walk(tree))
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerTests)
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful(): raise SystemExit(1)
