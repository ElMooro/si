"""Invented whole-handler regressions for Compound input integrity."""
from copy import deepcopy
import contextlib
import gzip
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import socket
import sys
import types
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from ciss_vintage_test_support import load
from compound_numeric import calculate, number, select, InvalidNumber, CONTRACT, read_feed


class Storage:
    def __init__(self, objects):
        self.objects = deepcopy(objects)
        self.writes = {}
        self.reads = []

    def get_object(self, **kw):
        self.reads.append(kw['Key'])
        raw = self.objects.get(kw['Key'], {})
        if isinstance(raw, Exception):
            raise raw
        if not isinstance(raw, bytes):
            raw = json.dumps(raw).encode('utf-8')
        return {'Body': io.BytesIO(raw), 'ContentLength': len(raw)}

    def put_object(self, **kw):
        self.writes[kw['Key']] = json.loads(kw['Body'])


class Whole(unittest.TestCase):
    def test_whole_retained_predecessor_keeps_valid_scores_details_and_order(self):
        root = Path(__file__).resolve().parents[3]
        fixture = root / 'tests/fixtures/compound-pre-numeric-20261002.py'
        self.assertEqual(hashlib.sha256(fixture.read_bytes()).hexdigest(), 'd38c26e9055b9bf52e7fab92d0597c8cc72c03835d92f4848ab4152976332b40')
        objects = {
            'data/nobrainers.json': {'summary': {'top_25_overall': [{'ticker': f'QA{i}', 'score': i / 10} for i in range(200)]}},
            'data/insider-clusters.json': {'clusters': [{'ticker': f'QA{i}', 'score': 60 - i / 10} for i in range(200)]},
            'data/deep-value.json': {'summary': {'top_25_overall': [{'symbol': f'QA{i}', 'score': i * 3} for i in range(0, 200, 3)]}}}
        with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline only')):
            current = load('justhodl-compound-aggregator')
            with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: None)}):
                spec = importlib.util.spec_from_file_location('compound_pre_numeric_fixture', fixture)
                prior = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(prior)
            packets = []
            for module in (prior, current):
                db = Storage(objects)
                with patch.object(module, 'S3', db), patch.object(module, 'emit_alerts', side_effect=AssertionError('No sends')), contextlib.redirect_stdout(io.StringIO()):
                    module.lambda_handler({'suppress_alerts': True})
                packets.append(db.writes[module.S3_KEY])
        added = {'score_calculation', 'calls_eligible', 'sizing_eligible', 'forecast_qualified', 'independent_evidence_eligible'}
        self.assertEqual(packets[0]['compound'], [{k: v for k, v in row.items() if k not in added} for row in packets[1]['compound']])
        self.assertEqual(packets[0]['stats'], packets[1]['stats'])
        self.assertEqual(packets[0]['feed_stats'], packets[1]['feed_stats'])

    def run_engine(self, rows, extras=None):
        objects = {'data/nobrainers.json': {'summary': {'top_25_overall': rows}},
                   'data/insider-clusters.json': {'clusters': [{'ticker': 'QAONLY', 'score': 10}]}}
        objects.update(extras or {})
        db = Storage(objects)
        with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline only')):
            m = load('justhodl-compound-aggregator')
            with patch.object(m, 'S3', db), patch.object(m, 'emit_alerts', side_effect=AssertionError('No sends')), contextlib.redirect_stdout(io.StringIO()):
                result = m.lambda_handler({'suppress_alerts': True})
        self.assertEqual(result['statusCode'], 200)
        self.assertNotIn(m.STATE_KEY, db.writes)
        for value in db.writes.values():
            json.dumps(value, allow_nan=False)
        return db.writes[m.S3_KEY], db

    def test_actual_zero_wins_over_present_fallback(self):
        packet, _ = self.run_engine([{'ticker': 'QAONLY', 'score': 0, 'asymmetric_score': 100}])
        row = packet['compound'][0]
        self.assertEqual(row['compound_score'], 15)
        self.assertEqual(row['scores']['nobrainers'], 0)
        calculation = row['score_calculation']
        self.assertEqual(calculation['sum_scores'], 10)
        self.assertEqual(calculation['multiplier'], 1.5)
        self.assertEqual(calculation['components'][0]['input']['score_field'], 'score')
        self.assertFalse(calculation['portfolio_qualified'])

    def test_absent_primary_may_use_explicit_fallback(self):
        packet, _ = self.run_engine([{'ticker': 'QAONLY', 'asymmetric_score': 100}])
        self.assertEqual(packet['compound'][0]['compound_score'], 165)
        self.assertEqual(packet['compound'][0]['score_calculation']['components'][0]['input']['score_field'], 'asymmetric_score')

    def test_present_invalid_primary_never_revives_alias(self):
        for value in (None, True, False, '', ' ', [], {}, 'NaN', 'Infinity', '1e-9999', '9007199254740993'):
            with self.subTest(value=value):
                packet, _ = self.run_engine([{'ticker': 'QAONLY', 'score': value, 'asymmetric_score': 100}])
                self.assertEqual(packet['compound'], [])
                self.assertEqual(packet['withheld'][0]['symbol'], 'QAONLY')
                self.assertEqual(packet['input_evidence']['nobrainers']['occurrences'][0]['record']['score'], value)

    def test_missing_score_is_not_a_measured_zero(self):
        packet, _ = self.run_engine([{'ticker': 'QAONLY'}])
        self.assertEqual(packet['compound'], [])
        self.assertEqual(packet['input_evidence']['nobrainers']['occurrences'][0]['reason'], 'score_missing')

    def test_numeric_string_is_explicitly_converted_and_original_preserved(self):
        packet, _ = self.run_engine([{'ticker': 'QAONLY', 'score': '7'}])
        row = packet['compound'][0]
        self.assertEqual(row['compound_score'], 25.5)
        self.assertEqual(row['score_calculation']['components'][0]['input']['record']['score'], '7')

    def test_all_duplicate_occurrences_retained_without_a_winner(self):
        rows = [{'ticker': 'QAONLY', 'score': 5}, {'ticker': ' qaonly ', 'score': 30}]
        packet, _ = self.run_engine(rows)
        self.assertEqual(packet['compound'], [])
        evidence = packet['input_evidence']['nobrainers']
        self.assertEqual(evidence['selected_count'], 2)
        self.assertEqual(evidence['usable_count'], 0)
        self.assertEqual([x['record'] for x in evidence['occurrences']], rows)
        self.assertEqual([x['pointer'] for x in evidence['occurrences']], ['/summary/top_25_overall/0', '/summary/top_25_overall/1'])

    def test_known_invalid_component_cannot_be_dropped_to_rank_other_votes(self):
        packet, _ = self.run_engine([{'ticker': 'QAONLY', 'score': None}],
            {'data/deep-value.json': {'summary': {'top_25_overall': [{'symbol': 'QAONLY', 'score': 90}]}}})
        self.assertEqual(packet['compound'], [])
        self.assertEqual(packet['withheld'][0]['symbol'], 'QAONLY')

    def test_source_nan_infinity_duplicate_key_and_lossy_tokens_are_unavailable(self):
        for raw in (b'{"summary":{"top_25_overall":[{"ticker":"QAONLY","score":NaN}]}}',
                    b'{"summary":{"top_25_overall":[{"ticker":"QAONLY","score":Infinity}]}}',
                    b'{"summary":{"top_25_overall":[{"ticker":"QAONLY","score":0,"score":100}]}}',
                    b'{"summary":{"top_25_overall":[{"ticker":"QAONLY","score":1e-9999}]}}'):
            with self.subTest(raw=raw):
                packet, _ = self.run_engine([], {'data/nobrainers.json': raw})
                self.assertEqual(packet['compound'], [])
                self.assertEqual(packet['input_evidence']['nobrainers']['status'], 'unavailable')

    def test_denial_is_not_successful_empty_and_does_not_leak(self):
        packet, _ = self.run_engine([], {'data/nobrainers.json': PermissionError('DO_NOT_EXPOSE')})
        self.assertEqual(packet['input_evidence']['nobrainers']['status'], 'unavailable')
        self.assertNotIn('DO_NOT_EXPOSE', json.dumps(packet))
        empty, _ = self.run_engine([])
        self.assertEqual(empty['input_evidence']['nobrainers']['status'], 'empty')

    def test_overflow_withholds_calculation_without_nonfinite_publication(self):
        packet, _ = self.run_engine([{'ticker': 'QAONLY', 'score': 1e308}],
            {'data/insider-clusters.json': {'clusters': [{'ticker': 'QAONLY', 'score': 1e308}]}})
        self.assertEqual(packet['compound'], [])
        self.assertEqual(packet['withheld'][0]['reason'], 'sum_overflow')

    def test_complete_input_record_and_unmodified_source_retained(self):
        row = {'ticker': 'QAONLY', 'score': 30, 'unmodeled': {'nested': [0, None, 'x']}}
        packet, _ = self.run_engine([row])
        evidence = packet['input_evidence']['nobrainers']
        self.assertEqual(evidence['occurrences'][0]['record'], row)
        self.assertNotIn('_normalized_symbol', evidence['occurrences'][0]['record'])
        self.assertTrue(evidence['whole_source_parsed'])
        self.assertFalse(evidence['original_bytes_retained'])
        self.assertEqual(packet['compound'][0]['score_calculation']['components'][0]['input']['record'], row)

    def test_numeric_contract_separates_history_cohorts_without_rewriting_old_days(self):
        m = load('justhodl-compound-aggregator')
        old = {'d': '2020-01-01', 'score_basis': m.BASIS, 'activist_boundary': 'ownership-feed-abstention.v1',
               'volatility_boundary': 'price-compression-abstention.v1', 'momentum_boundary': m.MOMENTUM_BASIS,
               'scores': {'QAONLY': 1}}
        packet, db = self.run_engine([{'ticker': 'QAONLY', 'score': 40}], {'data/compound-history.json': {'days': [old]}})
        self.assertNotIn('pctile_90d_all', packet['compound'][0])
        self.assertEqual(db.writes['data/compound-history.json']['days'][0], old)
        self.assertEqual(db.writes['data/compound-history.json']['days'][-1]['numeric_contract'], CONTRACT)

    def test_invalid_symbol_and_nonobject_row_are_diagnostic_not_crashes(self):
        packet, _ = self.run_engine([None, 9, {'ticker': 42, 'score': 10}, {'ticker': 'QAONLY', 'score': 30}])
        self.assertEqual(packet['compound'][0]['compound_score'], 60)
        evidence = packet['input_evidence']['nobrainers']
        self.assertEqual(evidence['selected_count'], 4)
        self.assertEqual(evidence['usable_count'], 1)
        self.assertEqual(len(evidence['occurrences']), 4)

    def test_serialization_failure_cannot_advance_any_secondary_document(self):
        objects = {'data/nobrainers.json': {'summary': {'top_25_overall': [{'ticker': 'QAONLY', 'score': 30}]}},
                   'data/insider-clusters.json': {'clusters': [{'ticker': 'QAONLY', 'score': 10}]},
                   'data/risk-gate.json': {'posture': float('nan')}}
        db = Storage(objects)
        with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline only')):
            m = load('justhodl-compound-aggregator')
            with patch.object(m, 'S3', db), patch.object(m, 'emit_alerts', side_effect=AssertionError('No sends')), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(ValueError):
                    m.lambda_handler({'suppress_alerts': True})
        self.assertEqual(db.writes, {})

    def test_notification_text_does_not_claim_independence_and_escapes_markup(self):
        m = load('justhodl-compound-aggregator')
        captured = []
        def memory_message(text):
            captured.append(text)
            return True, 'invented-no-delivery'
        with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline only')), patch.object(m, 'TELEGRAM_ENABLED', True), patch.object(m, 'send_telegram', side_effect=memory_message), contextlib.redirect_stdout(io.StringIO()):
            m.emit_alerts([{'symbol': 'QAONLY', 'type': 'TIER_3_EMERGED', 'n_systems': 3, 'score': 250, 'systems': []}], {'ranked': []})
        self.assertEqual(len(captured), 1)
        self.assertIn(r'\(independence unverified\)', captured[0])
        self.assertNotIn('independent systems agree', captured[0])


class Pure(unittest.TestCase):
    def test_collection_absence_malformed_and_empty_are_distinct(self):
        for packet, reason in (({}, 'collection_missing'), ({'rows': None}, 'collection_not_array')):
            self.assertEqual(select(packet, 'invented', 'rows', 'ticker').evidence['reason'], reason)
        self.assertEqual(select({'rows': []}, 'invented', 'rows', 'ticker').evidence['status'], 'empty')

    def test_large_collection_is_never_silently_sliced(self):
        rows = [{'ticker': 'Q'+str(i), 'score': i} for i in range(300)]
        original = deepcopy(rows)
        result = select({'rows': rows}, 'invented', 'rows', 'ticker')
        self.assertEqual(len(result), 300)
        self.assertEqual(len(result.evidence['occurrences']), 300)
        self.assertEqual(rows, original)

    def test_valid_numbers_and_signed_scores_keep_the_legacy_formula(self):
        for scores in ({'a': 0, 'b': 0}, {'a': -10, 'b': 5}, {'a': 40, 'b': 60, 'c': 25}):
            result = calculate(scores, {s: {'source': s} for s in scores})
            self.assertEqual(result['score'], round(sum(scores.values()) * (1 + .5*(len(scores)-1)), 1))
            self.assertEqual(result['arithmetic_order'], list(scores))
        for value in (True, False, None, float('nan'), float('inf'), '1e-9999', 9007199254740993):
            with self.subTest(value=value), self.assertRaises(InvalidNumber):
                number(value)

    def test_source_stream_length_and_encoding_are_verified_and_closed(self):
        class Client:
            def __init__(self, raw, **metadata):
                self.body = io.BytesIO(raw)
                self.metadata = metadata
            def get_object(self, **kw):
                return {'Body': self.body, **self.metadata}
        raw = b'{"rows":[{"ticker":"Q","score":0}]}'
        for client in (Client(raw), Client(raw, ContentLength=True), Client(raw, ContentLength=len(raw)-1),
                       Client(raw, ContentLength=len(raw), ContentEncoding='gzip'),
                       Client(raw, ContentLength=len(raw), ContentEncoding='br')):
            self.assertEqual(read_feed(client, 'invented', 'invented', 'rows', 'ticker').evidence['status'], 'unavailable')
            self.assertTrue(client.body.closed)
        for data in (raw, gzip.compress(raw)):
            client = Client(data, ContentLength=len(data))
            rows = read_feed(client, 'invented', 'invented', 'rows', 'ticker')
            self.assertEqual(rows[0]['_compound_score'], 0)
            self.assertTrue(client.body.closed)


if __name__ == '__main__':
    unittest.main()
