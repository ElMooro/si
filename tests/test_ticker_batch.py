"""Batch optimization against the unchanged single-ticker behavioral oracle."""
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import copy
import io
import json
import random
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import ticker_360 as hub
import ticker_360_batch as batch


class Clock:
    @staticmethod
    def now(*args):
        return datetime(2026, 10, 7, 18, 0, tzinfo=timezone.utc)


class Storage:
    def __init__(self, values):
        self.values = values
        self.reads = []
        self.writes = []

    def get_object(self, **request):
        self.reads.append(request['Key'])
        value = self.values[request['Key']]
        if isinstance(value, Exception):
            raise value
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        return {'Body': io.BytesIO(raw), 'ContentLength': len(raw)}

    def put_object(self, **request):
        self.writes.append(request)
        return {}


def source(key='fixture', kind='packet', context=None, **extras):
    return dict(key=key, kind=kind, context=context, **extras)


class Batch(unittest.TestCase):
    def compare(self, packet, tickers=('ABC', 'OTHER', 'NONE', ''), spec=None, domain='fixture', loader=None):
        spec = spec or source()
        before = copy.deepcopy(packet)
        storage = Storage({'fixture': packet})
        with patch.object(hub, 'SOURCES', {domain: spec}), patch.object(hub, 'datetime', Clock), patch.object(batch, 'datetime', Clock):
            with patch.dict(hub.CONTEXT_LOADERS, {} if loader is None else {'invented': loader}):
                expected = [(t, hub.enrich(t, storage)) for t in tickers]
                actual = list(batch.iter_enriched(tickers, storage))
        self.assertEqual(actual, expected)
        self.assertEqual(packet, before)
        return actual

    def test_precedence_null_duplicates_empty_maps_and_list_aliases(self):
        cases = [
            {'by_ticker': {'abc': None, 'ABC': {'wrong': 1}}, 'rows': [{'ticker': 'ABC', 'wrong': 2}]},
            {'by_ticker': [{'ticker': 'ABC', 'wrong': 1}], 'tickers': {'abc': {'value': 0}}},
            {'by_ticker': {}, 'tickers': {'abc': 0, 'OTHER': False}},
            {'tickers': ['ABC', 'OTHER'], 'rows': [{'ticker': 'ABC', 'wrong': 2}]},
            {'ABC': {'value': 0}, 'abc': {'value': 1}, 'OTHER': 0},
            {'tickers': {}, 'by_ticker': {'ABC': {'value': 2}}},
        ]
        for kind in ('packet', 'tkr'):
            for packet in cases:
                with self.subTest(kind=kind, packet=packet):
                    self.compare(packet, spec=source(kind=kind))
        for key in hub._TICKER_LIST_KEYS:
            self.compare({key: [{'ticker': 'ABC', 'value': 0}, {'ticker': 'ABC', 'wrong': 1}, ' OTHER ', 0]})

    def test_seeded_mixed_schema_equivalence(self):
        rng = random.Random(731)
        rows = [None, False, 0, '', {}, {'ticker': 'abc'}, {'symbol': 'OTHER'}, {'ticker': ' ABC '}, 'abc', 12]
        for _ in range(250):
            packet = {}
            for key in rng.sample(list(hub._TICKER_LIST_KEYS), 6):
                packet[key] = (rng.sample(rows, 5) if rng.randrange(2) else
                               {key: copy.deepcopy(rng.choice(rows)) for key in ('abc', 'ABC', 'OTHER', '')})
            self.compare(packet)

    def test_every_real_guard_keeps_legacy_rows_withheld(self):
        packet = {'by_ticker': {'ABC': {'score': 99}}, 'generated_at': '2020-01-01', 'as_of': '1999-12-31'}
        for context in hub.CONTEXT_LOADERS:
            with self.subTest(context=context):
                actual = self.compare(packet, spec=source(context=context))
                for _, view in actual:
                    domain = view['domains']['fixture']
                    self.assertIsNone(domain['ticker_data'])
                    self.assertFalse(domain['calls_eligible'])
                    self.assertFalse(domain['raw_fallback_used'])

    def test_failed_invalid_and_withheld_context_never_falls_back(self):
        packet = {'by_ticker': {'ABC': {'score': 99}}}
        def failed(_):
            raise ValueError('PRIVATE_CANARY')
        for loader in (failed, lambda _: None, lambda _: [], lambda _: {'by_ticker': {}, 'research_context': {'calls_eligible': False}}):
            result = self.compare(packet, spec=source(context='invented'), domain='dollar', loader=loader)
            self.assertNotIn('PRIVATE_CANARY', json.dumps(result))
        self.compare(packet, spec=source(context='not_registered'), domain='dollar')

    def test_projected_rows_and_market_wide_placeholders_match(self):
        self.compare({'by_ticker': {'ABC': {'wrong': 99}}}, spec=source(context='invented'),
                     loader=lambda _: {'rows': [{'ticker': 'ABC', 'reported': 0}], 'score': None, 'research_context': {'status': 'hold'}})
        self.compare({'score': 0, 'as_of': False, 'generated_at': '2020'}, domain='macro-regime')

    def test_fallback_identity_and_strict_failed_source_boundary(self):
        values = {'primary': b'{"value":NaN}', 'fallback': {'by_ticker': {'ABC': {'value': 0}}}, 'missing': OSError('PRIVATE_CANARY')}
        sources = {'good': source('primary', fallback_key='fallback'), 'missing': source('missing')}
        with patch.object(hub, 'SOURCES', sources), patch.object(hub, 'datetime', Clock), patch.object(batch, 'datetime', Clock):
            expected = hub.enrich('ABC', Storage(values))
            storage = Storage(values)
            result = list(batch.iter_enriched(['ABC', 'OTHER'], storage))
        self.assertEqual(result[0][1], expected)
        self.assertEqual(storage.reads, ['primary', 'fallback', 'missing'])
        self.assertEqual(result[0][1]['domains']['good']['source_key'], 'fallback')
        self.assertNotIn('PRIVATE_CANARY', json.dumps(result))

    def test_validation_cost_is_per_source_and_no_cross_invocation_cache(self):
        calls = []
        def project(packet):
            calls.append(packet['value'])
            return {'rows': [{'ticker': 'ABC', 'value': packet['value']}]}
        storage = Storage({'fixture': {'value': 0}})
        with patch.object(hub, 'SOURCES', {'fixture': source(context='invented')}), patch.dict(hub.CONTEXT_LOADERS, invented=project):
            result = list(batch.iter_enriched(['ABC'] * 3145, storage))
            self.assertEqual(calls, [0])
            self.assertEqual(storage.reads, ['fixture'])
            storage.values['fixture']['value'] = 1
            next_result = list(batch.iter_enriched(['ABC'], storage))
        self.assertEqual(calls, [0, 1])
        self.assertEqual(result[0][1]['domains']['fixture']['ticker_data']['value'], 0)
        self.assertEqual(next_result[0][1]['domains']['fixture']['ticker_data']['value'], 1)

    def test_complete_producer_output_matches_retained_predecessor(self):
        from ticker_batch_preservation import preceding_source
        path = ROOT / 'aws/lambdas/justhodl-ticker-360/source/lambda_function.py'
        new = path.read_text(encoding='utf-8')
        old = preceding_source(path, new)
        rows = [{'ticker': 'T' + str(n), 'value': n, 'extra': [0, {'nested': 'value'}]} for n in range(64)]
        values = {'fixture': {'stocks': rows, 'generated_at': '2020', 'as_of': '1999'}, 'market': {'score': None}}
        sources = {'fixture': source(), 'macro-regime': source('market')}
        outputs = []
        for text in (old, new):
            storage = Storage(values)
            boto = types.ModuleType('boto3')
            boto.client = lambda *a, **kw: storage
            module = types.ModuleType('producer')
            with patch.dict(sys.modules, boto3=boto), patch.object(hub, 'SOURCES', sources):
                exec(compile(text, str(path), 'exec'), module.__dict__)
                with patch.object(module, 'datetime', Clock), patch.object(module.time, 'time', return_value=100), patch.object(module, 'publish_network', return_value={'publication_id': 'fixture', 'entity_count': 64}) as network:
                    outcome = module.lambda_handler({}, None)
                    network.assert_called_once_with(storage, module.BUCKET, context=None)
                outputs.append((outcome, storage.writes, storage.reads))
        self.assertEqual(outputs[0], outputs[1])


if __name__ == '__main__':
    unittest.main(verbosity=2)
