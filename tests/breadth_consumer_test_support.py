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
            data = ss.fetch('BREADTH_NATIVE', 'ADVANCERS')
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
        self.assertEqual(ss.map_symbol('JH_BREADTH:ADVANCERS')[:2], ('BREADTH_NATIVE', 'ADVANCERS'))

    def test_old_cached_source_and_derived_aliases_do_not_read_the_native_packet(self):
        with patch.object(ss, '_internals', side_effect=AssertionError('legacy alias must not read new scope')):
            self.assertEqual(ss.fetch('INTERNALS', 'ADVANCERS'), {})
            self.assertEqual(ss.fetch('DERIVED', 'INTERNALS~ADVDEC_LINE~mcclellan_osc'), {})

    def test_cached_maps_and_dependent_formulas_are_withheld_without_mutating_archive(self):
        mappings={'USI:ADVQ':{'source':'INTERNALS','id':'ADVANCERS'},
            'CUSTOM:A':{'source':'INTERNALS','id':'ADVANCERS'},
            'CUSTOM:B':{'source':'FORMULA','id':'CUSTOM:A+FRED:SOFR'},
            'CUSTOM:C':{'source':'FORMULA','id':'CUSTOM:B*2'},
            'FRED:SOFR':{'source':'FRED','id':'SOFR'},
            'JH_BREADTH:ADVANCERS':{'source':'BREADTH_NATIVE','id':'ADVANCERS'}}
        before=json.dumps(mappings,sort_keys=True);accepted,withheld=ss.filter_mappings(mappings)
        self.assertEqual(set(accepted), {'FRED:SOFR','JH_BREADTH:ADVANCERS'})
        self.assertEqual(withheld, {'USI:ADVQ','CUSTOM:A','CUSTOM:B','CUSTOM:C'})
        self.assertEqual(json.dumps(mappings,sort_keys=True),before)

    def test_signal_board_abstains_even_when_a_producer_claims_authority(self):
        tree = ast.parse((ROOT/'aws/lambdas/justhodl-signal-board/source/lambda_function.py').read_text(encoding='utf-8'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'n_market_internals')
        scope = {}; exec(compile(ast.Module(body=[node], type_ignores=[]), '<reviewed normalizer>', 'exec'), scope)
        for p in ({}, {'calls_eligible': True, 'mcclellan': {'oscillator': -200}, 'zweig_thrust': {'fired': True}}):
            self.assertIsNone(scope['n_market_internals'](p)[0])

    def test_dictionary_marks_old_cached_alias_as_unverified_without_a_provider_lookup(self):
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-symbol-dictionary/source/lambda_function.py').read_text(encoding='utf-8'))
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        node=next(n for n in handler.body if isinstance(n,ast.FunctionDef) and n.name=='resolve')
        scope={'smap':{},'withheld_breadth_mappings':{'USI:ADVQ'}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'<reviewed dictionary resolver>','exec'),scope)
        sym,row=scope['resolve']('USI:ADVQ')
        self.assertEqual(sym,'USI:ADVQ');self.assertIsNone(row['source']);self.assertIsNone(row['source_id'])
        self.assertTrue(row['provisional']);self.assertEqual(row['category'],'unverified_source_scope')


def run(function):
    tree = ast.parse((ROOT/'aws/lambdas'/function/'source/lambda_function.py').read_text(encoding='utf-8'))
    assert any(isinstance(n, ast.Import) and any(a.name == 'series_source' for a in n.names) for n in ast.walk(tree))
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerTests)
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful(): raise SystemExit(1)
