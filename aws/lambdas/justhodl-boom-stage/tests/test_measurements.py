"""Independent calendar/unit cases and complete native FRED acquisition path."""
from copy import deepcopy
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
import ast
import hashlib
import json
import unittest
import urllib.error
import urllib.parse

import boom_history as h
import boom_measurements as m
from run_tests import Memory, NOW, native, SOURCE, ROOT


def meta(sid='TCU'):
    unit, frequency, seasonal, *_ = m.PROFILES.get(sid, ('Unverified', 'W', 'NSA'))
    return {'seriess': [{'id': sid, 'title': 'Original provider title', 'units': unit,
                        'frequency_short': frequency, 'seasonal_adjustment_short': seasonal,
                        'unknown_preserve': [0, None, 'complete']}], 'unknown': {'all': True}}


def packet(rows=None):
    rows = rows if rows is not None else [{'date': '2026-08-01', 'value': '76.2717'},
                                        {'date': '2025-08-01', 'value': '74.0017'}]
    return {'observations': rows, 'count': len(rows), 'offset': 0, 'units': 'lin', 'output_type': 1,
            'unknown_preserve': [None, 0, {'whole': True}]}


class Response(BytesIO):
    def __init__(self, raw, status=200, length=None):
        super().__init__(raw); self.status = status
        self.headers = {'Content-Length': str(len(raw) if length is None else length)}


class Provider:
    def __init__(self): self.requests = []; self.raw = []
    def __call__(self, request, timeout):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        sid = query['series_id'][0]
        self.requests.append((request.full_url, timeout))
        value = packet() if '/observations?' in request.full_url else meta(sid)
        raw = h.encode(value); self.raw.append(raw)
        return Response(raw)


class Measurements(unittest.TestCase):
    def test_calendar_not_row_position_missing_months_and_zero(self):
        rows = [{'date': '2026-08-01', 'value': '76.2717'}, {'date': '2026-07-01', 'value': '.'},
                {'date': '2025-08-01', 'value': '74.0017'}, {'date': '2025-07-01', 'value': '100'}]
        result = m.measure('TCU', meta(), packet(rows), NOW.isoformat())
        self.assertEqual((result['date'], result['prior_date'], result['yoy_chg']), ('2026-08-01', '2025-08-01', 2.27))
        self.assertEqual(result['change_unit'], 'percentage_points')
        self.assertEqual(len(result['observations']), 4)
        self.assertIsNone(result['observations'][1]['value'])
        rows[2]['value'] = '0'
        self.assertEqual(m.measure('TCU', meta(), packet(rows), NOW.isoformat())['yoy_chg'], 76.2717)
        rows[0]['value'] = '0'
        zero = m.measure('TCU', meta(), packet(rows), NOW.isoformat())
        self.assertEqual((zero['level'], zero['prior_level'], zero['yoy_chg']), (0, 0, 0))
        rows[0]['value'] = '.'
        missing = m.measure('TCU', meta(), packet(rows), NOW.isoformat())
        self.assertEqual(missing['date'], '2026-08-01'); self.assertIsNone(missing['level'])
        self.assertEqual(missing['status'], 'latest_missing')

    def test_wrong_year_or_12_week_position_cannot_be_yoy(self):
        result = m.measure('TCU', meta(), packet([{'date': '2026-08-01', 'value': '5'}, {'date': '2025-07-01', 'value': '1'}]), NOW.isoformat())
        self.assertIsNone(result['yoy_chg']); self.assertEqual(result['status'], 'prior_month_missing')
        changed = meta(); changed['seriess'][0]['frequency_short'] = 'W'
        self.assertEqual(m.measure('TCU', changed, packet(), NOW.isoformat())['status'], 'metadata_definition_changed')
        for sid in m.UNREVIEWED:
            result = m.measure(sid, meta(sid), packet(), NOW.isoformat())
            self.assertIsNone(result['level']); self.assertIsNone(result['yoy_chg'])
            self.assertEqual(result['status'], 'definition_unverified')

    def test_ratio_is_ratio_points_not_stocks_and_all_mappings_unqualified(self):
        rows = [{'date': '2026-07-01', 'value': '1.30'}, {'date': '2025-07-01', 'value': '1.26'}]
        out = m.measure('ISRATIO', meta('ISRATIO'), packet(rows), NOW.isoformat())
        self.assertEqual((out['unit'], out['change_unit'], out['yoy_chg']), ('Ratio', 'ratio_points', .04))
        self.assertEqual(out['read'], 'RESEARCH_ONLY')
        self.assertFalse(out['country_industry_mapping_qualified'])
        self.assertTrue(all(out[f] is False for f in h.FLAGS))
        mod = native(Memory())
        self.assertEqual(mod._refine('EARLY_PRICE_LED', out), ('EARLY_PRICE_LED', None))

    def test_duplicate_future_invalid_and_out_of_window_rows_stop_comparison(self):
        for extra in ({'date': '2026-08-01', 'value': '9'}, {'date': '2026-10-01', 'value': '9'},
                      {'date': '2026-02-30', 'value': '9'}, {'date': '2026-08-02', 'value': '9'},
                      {'date': '0001-01-01', 'value': '9'}, None):
            rows = packet()['observations']+[extra]
            out = m.measure('TCU', meta(), packet(rows), NOW.isoformat())
            self.assertEqual(out['status'], 'ambiguous_observation_identity')
            self.assertIsNone(out['yoy_chg']); self.assertEqual(len(out['observations']), 3)
        for value in (True, -1, 'NaN', 'Infinity', '1e400', {}, [], None):
            rows = packet()['observations']; rows[0]['value'] = value
            out = m.measure('TCU', meta(), packet(rows), NOW.isoformat())
            self.assertEqual(out['status'], 'latest_missing')

    def test_definition_count_aggregation_and_duplicate_json_are_checked(self):
        for key, val in (('count', 999), ('offset', 1), ('units', 'pch'), ('output_type', True)):
            body = packet(); body[key] = val
            self.assertEqual(m.measure('TCU', meta(), body, NOW.isoformat())['status'], 'incomplete_or_transformed_response')
        for key, val in (('id', 'ISRATIO'), ('units', 'Ratio'), ('seasonal_adjustment_short', 'NSA')):
            body = meta(); body['seriess'][0][key] = val
            self.assertNotEqual(m.measure('TCU', body, packet(), NOW.isoformat())['status'], 'measured')
        mem = Memory(); raw = b'{"seriess":[],"seriess":[]}'
        session = m.Session(mem, 'bucket', NOW.isoformat(), 'synthetic-test-credential', lambda *a, **kw: Response(raw))
        result = session.get('TCU')
        self.assertEqual(result['original_responses']['metadata']['status'], 'malformed_complete_json')
        self.assertEqual(mem.data[h.PRIVATE+h.sha(raw)+'.bin'], raw)

    def test_whole_responses_cached_once_retained_before_parse_and_replayed(self):
        mem, provider = Memory(), Provider()
        session = m.Session(mem, 'bucket', NOW.isoformat(), 'synthetic-test-credential', provider)
        out = session.get('TCU'); out['level'] = 999
        self.assertEqual(session.get('TCU')['level'], 76.2717)
        self.assertEqual(len(provider.requests), 2)
        for url, timeout in provider.requests:
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            self.assertEqual(query['realtime_end'], ['2026-09-27'])
            self.assertLessEqual(timeout, 5)
        for raw in provider.raw: self.assertEqual(mem.data[h.PRIVATE+h.sha(raw)+'.bin'], raw)
        review = session.review()
        self.assertNotIn('synthetic-test-credential', json.dumps(review))
        self.assertNotIn('api_key', json.dumps(review))
        read = lambda ref: mem.data[ref['key']]
        self.assertTrue(m.replay(review, read)['complete_dated_arithmetic_matches'])
        with self.assertRaises(h.IntegrityError): m.replay(review, lambda ref: b'{}')
        review['series']['TCU']['yoy_chg'] = 999
        with self.assertRaises(h.IntegrityError): m.replay(review, read)
        self.assertEqual(mem.public_writes, [])
        # A long pause for another provider must not exhaust the FRED budget.
        session = m.Session(Memory(), 'bucket', NOW.isoformat(), 'fixture', Provider())
        with patch.object(m.time, 'monotonic', side_effect=[100, 102, 300, 303]):
            self.assertEqual(session.get('TCU')['status'], 'measured')
        self.assertEqual(session.remaining, 35)

    def test_retention_truncation_http_transport_and_budget_failure(self):
        mem = Memory(); mem.fail_private = True
        with self.assertRaises(h.IntegrityError): m.Session(mem, 'bucket', NOW.isoformat(), 'fixture', Provider()).get('TCU')
        mem = Memory()
        with self.assertRaises(h.IntegrityError):
            m.Session(mem, 'bucket', NOW.isoformat(), 'fixture', lambda *a, **kw: Response(b'{}', length=50)).get('TCU')
        raw = b'whole unavailable response'
        result = m.Session(mem, 'bucket', NOW.isoformat(), 'fixture', lambda *a, **kw: Response(raw, 429)).get('TCU')
        self.assertEqual(result['original_responses']['metadata']['http_status'], 429)
        self.assertEqual(mem.data[h.PRIVATE+h.sha(raw)+'.bin'], raw)
        def fail(*a, **kw): raise RuntimeError('secret must not escape')
        result = m.Session(mem, 'bucket', NOW.isoformat(), 'fixture', fail).get('TCU')
        self.assertNotIn('secret', json.dumps(result))
        session = m.Session(mem, 'bucket', NOW.isoformat(), 'fixture', fail); session.remaining = 0
        self.assertEqual(session.get('TCU')['original_responses']['metadata']['status'], 'request_budget_exhausted')
        with self.assertRaises(ValueError): m.NoRedirect().redirect_request(None, None, None, None, None, None)
        self.assertEqual(mem.public_writes, [])

    def test_complete_native_handler_uses_six_unique_sources_and_keeps_120_dates(self):
        mem, provider = Memory(), Provider()
        mod = native(mem, mock_factor=False); mod.FRED_KEY = 'synthetic-test-credential'
        real_session = m.Session
        with patch.object(m, 'Session', side_effect=lambda *a: real_session(*a, opener=provider)):
            mod.lambda_handler()
        out = h.decode(mem.data[h.HEAD]); review = out['fred_measurements']
        self.assertEqual(set(review['series']), {'CAPUTLG3344S', 'CAPUTLG21S', 'TCU', 'WGTSTUS1', 'ISRATIO', 'RETAILIRSA'})
        self.assertEqual(len(provider.requests), 12)
        self.assertEqual(len(out['pairs']), 20)
        self.assertEqual(len(h.decode(mem.data[h.HISTORY])['days']), 121)
        self.assertEqual(out['macro']['inventory_tilt'], 0)
        self.assertTrue(all(p['factor4']['read'] == 'RESEARCH_ONLY' for p in out['pairs'] if p.get('factor4')))
        self.assertTrue(all(p.get('factor4_note') is None for p in out['pairs']))
        self.assertEqual(len(out['history_preservation']['compiler_sha256']), 4)
        self.assertEqual(m.replay(review, lambda ref: mem.data[ref['key']])['series'], 6)

    def test_complete_predecessor_and_unrelated_functions_preserved(self):
        old = (ROOT/'tests/fixtures/pre-calendar-boom-stage.py.txt').read_bytes()
        self.assertEqual(len(old), 41027)
        self.assertEqual(hashlib.sha256(old).hexdigest(), 'd081241e948bc7aea25c40af322688b32333c762b260c2d57547ff21650948a7')
        funcs = lambda raw: {n.name: ast.dump(n) for n in ast.parse(raw).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
        before, after = funcs(old), funcs((SOURCE/'lambda_function.py').read_bytes())
        for name in before:
            if name not in ('lambda_handler', '_fred_pair', '_factor4'): self.assertEqual(before[name], after[name], name)
