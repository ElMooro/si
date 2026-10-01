from pathlib import Path
import copy
import hashlib
import importlib.util
import importlib.machinery
import io
import json
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
KEYS = ['data/short-interest-tickers.json', 'data/13f-positions.json',
        'data/estimate-revisions-latest.json', 'data/rotation-chains.json']


def deny(*args, **kwargs):
    raise AssertionError('External transport or model forbidden')


class ReadFailure(Exception):
    def __init__(self, response):
        self.response = response


class S3:
    def __init__(self, docs):
        self.docs = docs
        self.reads = []
        self.streams = []
        self.lengths = {}

    def get_object(self, **kwargs):
        assert kwargs['Bucket'] == 'justhodl-dashboard-live' and kwargs['Key'] in KEYS
        key = kwargs['Key']
        self.reads.append(key)
        value = self.docs[key]
        if isinstance(value, Exception):
            raise value
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        stream = io.BytesIO(raw)
        self.streams.append(stream)
        return {'Body': stream, 'ContentLength': self.lengths.get(key, len(raw))}


def healthy():
    return {KEYS[0]: {'contract': 'short-interest-tickers.v1', 'by_ticker': {'TEST': {'short_interest': 0}}},
            KEYS[1]: {'aggregate_by_ticker': {'TEST': {'n_funds_holding': 0}}},
            KEYS[2]: {'fwd_rev_growth': {'TEST': 0}},
            KEYS[3]: {'chains': {'invented': {'expected_catchup_pct': 0, 'leader_perf_30d_pct': 0,
                       'next_up_tickers': [{'ticker': 'TEST', 'own_30d_pct': 0}]}}}}


def load(client, source_path=None):
    context_spec = importlib.util.spec_from_file_location('short_position_context', ROOT / 'aws/shared/short_position_context.py')
    context_module = importlib.util.module_from_spec(context_spec)
    context_spec.loader.exec_module(context_module)
    fake = {'boto3': types.SimpleNamespace(client=lambda *a, **kw: client),
            'managed_secret': types.SimpleNamespace(managed_secret=lambda *a, **kw: None),
            'llm_router': types.SimpleNamespace(complete=deny),
            'short_position_context': context_module}
    path = source_path or ROOT / 'aws/shared/equity_enrich.py'
    spec = importlib.util.spec_from_file_location('whole_candidate_equity_loader', path,
        loader=importlib.machinery.SourceFileLoader('whole_candidate_equity_loader', str(path)))
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, fake), patch('urllib.request.urlopen', deny):
        spec.loader.exec_module(module)
    module._fixture_context = context_module
    return module


class WholeLoader(unittest.TestCase):
    def run_loader(self, docs=None, lengths=None, limit=None, descriptive=False):
        client = S3(healthy() if docs is None else docs)
        client.lengths.update(lengths or {})
        module = load(client)
        if limit is not None:
            module.CONFIRMATION_BODY_LIMIT = limit
        with patch('urllib.request.urlopen', deny), patch.dict(sys.modules, {'short_position_context': module._fixture_context}):
            result = module.load_confirmation_feeds(with_availability=True, descriptive_short_positions=descriptive)
        self.assertEqual(client.reads, KEYS)
        self.assertTrue(all(stream.closed for stream in client.streams))
        json.dumps(result, allow_nan=False)
        return module, result

    def test_healthy_legacy_values_and_real_zeros_preserved(self):
        _, result = self.run_loader()
        self.assertEqual(result[0]['TEST']['short_interest'], 0)
        self.assertEqual(result[1]['TEST']['n_funds_holding'], 0)
        self.assertEqual(result[2]['TEST'], 0)
        self.assertEqual(result[3]['TEST']['catchup'], 0)
        self.assertEqual(result[3]['TEST']['own_30d'], 0)
        self.assertEqual(result[3]['TEST']['leader_30d'], 0)
        self.assertTrue(all(m['read_status'] == 'parsed' for m in result[4].values()))

    def test_default_call_still_returns_four_mappings(self):
        module = load(S3(healthy()))
        with patch('urllib.request.urlopen', deny):
            self.assertEqual(len(module.load_confirmation_feeds()), 4)

    def test_opted_in_position_context_uses_reconciled_ratio_and_row_pointer(self):
        docs = healthy()
        docs[KEYS[0]]['by_ticker']['TEST'].update(days_to_cover='999', dtc_effective='2', dtc_reconstructed='2', dtc_status='provider_differs_from_reconstructed_ratio')
        _, result = self.run_loader(docs, descriptive=True)
        row = result[0]['TEST']
        self.assertEqual(row['days_to_cover'], 2)
        self.assertEqual(row['reported_days_to_cover'], 999)
        self.assertEqual(row['source_row'], '/by_ticker/TEST')
        self.assertIsNone(row['si_pct_float'])
        self.assertFalse(row['calls_eligible'])
        self.assertEqual(result[4]['short_interest']['projected_rows'], 1)
        self.assertEqual(result[1]['TEST']['n_funds_holding'], 0)

    def test_opted_in_position_context_quarantines_ambiguous_and_unknown_contracts(self):
        docs = healthy();docs[KEYS[0]]['by_ticker']['test'] = {'short_interest': 100}
        _, result = self.run_loader(docs, descriptive=True)
        self.assertEqual(result[0], {})
        self.assertEqual(result[4]['short_interest']['ambiguous_ticker_count'], 1)
        self.assertEqual(result[4]['short_interest']['excluded_rows'], 2)
        docs = healthy();docs[KEYS[0]]['contract'] = 'unrecognized'
        _, result = self.run_loader(docs, descriptive=True)
        self.assertEqual(result[0], {})
        self.assertEqual(result[4]['short_interest']['context_status'], 'unavailable')
        self.assertEqual(result[2]['TEST'], 0)

    def test_one_invalid_top_level_cannot_block_other_feeds(self):
        for key in KEYS:
            for value in ([1], 'broken', True, None):
                docs = healthy(); docs[key] = value
                with self.subTest(key=key, value=value):
                    _, result = self.run_loader(docs)
                    self.assertEqual(result[KEYS.index(key)], {})
                    self.assertEqual(sum(m['read_status'] == 'parsed' for m in result[4].values()), 3)

    def test_bad_mapping_or_row_never_causes_attribute_error(self):
        docs = healthy()
        docs[KEYS[0]]['by_ticker']['BAD'] = [1]
        docs[KEYS[1]]['aggregate_by_ticker']['BAD'] = 'broken'
        docs[KEYS[2]]['fwd_rev_growth'].update(BAD=True, TEXT='10', HUGE=10**1000)
        _, result = self.run_loader(docs)
        self.assertEqual([list(result[i]) for i in range(3)], [['TEST']] * 3)
        self.assertEqual(result[4]['estimate_revisions']['excluded_rows'], 3)
        docs[KEYS[0]]['by_ticker'] = []
        _, result = self.run_loader(docs)
        self.assertEqual(result[4]['short_interest']['mapping_status'], 'unavailable')

    def test_bad_chain_members_do_not_remove_the_valid_zero_row(self):
        docs = healthy(); rows = docs[KEYS[3]]['chains']['invented']['next_up_tickers']
        rows[:0] = [None, 1, 'bad', {}, {'ticker': []}, {'ticker': '  '}]
        _, result = self.run_loader(docs)
        self.assertEqual(list(result[3]), ['TEST'])
        self.assertEqual(result[3]['TEST']['catchup'], 0)

    def test_duplicate_nonfinite_and_invalid_utf8_bodies_are_not_parsed(self):
        for raw in [b'{"by_ticker":{},"by_ticker":{}}', b'{"by_ticker":{"TEST":{"x":NaN}}}',
                    b'{"by_ticker":{"TEST":{"x":1e1000}}}',
                    b'{"by_ticker":{"TEST":{"x":1e-1000}}}', b'\xff']:
            docs = healthy(); docs[KEYS[0]] = raw
            _, result = self.run_loader(docs)
            meta = result[4]['short_interest']
            self.assertEqual(meta['read_status'], 'malformed')
            self.assertEqual(meta['body_bytes'], len(raw))
            self.assertEqual(meta['body_sha256'], hashlib.sha256(raw).hexdigest())

    def test_complete_empty_body_retains_zero_byte_digest(self):
        docs = healthy(); docs[KEYS[0]] = b''
        _, result = self.run_loader(docs)
        meta = result[4]['short_interest']
        self.assertEqual(meta['read_status'], 'malformed')
        self.assertEqual(meta['body_bytes'], 0)
        self.assertEqual(meta['body_sha256'], hashlib.sha256(b'').hexdigest())

    def test_oversized_response_is_not_partially_published(self):
        _, result = self.run_loader(lengths={KEYS[0]: 1000}, limit=500)
        self.assertEqual(result[0], {})
        self.assertEqual(result[4]['short_interest']['read_status'], 'oversize')
        self.assertIsNone(result[4]['short_interest']['body_sha256'])

    def test_inconsistent_missing_or_boolean_length_cannot_claim_complete_body(self):
        for length in (None, True, 0, 1):
            _, result = self.run_loader(lengths={KEYS[0]: length})
            self.assertEqual(result[0], {})
            self.assertIsNone(result[4]['short_interest']['body_sha256'])

    def test_missing_and_denied_are_separate_without_disclosing_error_text(self):
        for response, expected in [({'Error': {'Code': 'NoSuchKey'}}, 'missing'),
                                   ({'Error': {'Code': 'AccessDenied', 'Message': 'PRIVATE_CANARY'}}, 'unavailable'),
                                   ({'Error': None}, 'unavailable')]:
            docs = healthy(); docs[KEYS[0]] = ReadFailure(response)
            _, result = self.run_loader(docs)
            self.assertEqual(result[4]['short_interest']['read_status'], expected)
            self.assertNotIn('PRIVATE_CANARY', json.dumps(result[4]))

    def test_reported_errors_and_unknown_contract_text_are_not_forwarded_in_metadata(self):
        docs = healthy(); docs[KEYS[0]] = {'contract': 'PRIVATE_CANARY', 'error': 'PRIVATE_CANARY'}
        _, result = self.run_loader(docs)
        self.assertEqual(result[0], {})
        self.assertEqual(result[4]['short_interest']['read_status'], 'reported_error')
        self.assertNotIn('PRIVATE_CANARY', json.dumps(result[4]))

    def test_availability_never_claims_body_replay_or_observation_qualification(self):
        _, result = self.run_loader()
        for meta in result[4].values():
            for key in ['original_body_retained', 'observation_freshness_verified', 'calls_eligible', 'sizing_eligible']:
                self.assertIs(meta[key], False)


if __name__ == '__main__':
    unittest.main(verbosity=2)
