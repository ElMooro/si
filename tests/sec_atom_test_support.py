"""Isolated complete-feed, predecessor, conditional publication and replay tests."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from types import ModuleType
from unittest.mock import patch
from xml.sax.saxutils import escape, quoteattr
import importlib.util
import sys
import unittest
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import sec_atom_model as model
import sec_atom_store as store

AT = '2026-09-27T16:00:00+00:00'
RECIPE = {'kind': '8k', 'window_days': 7, 'item_labels': {'4.02': 'Non-reliance', '2.02': 'Results'},
          'red_flag_items': ['4.02'], 'high_impact_items': ['2.02']}


def entry(n=1, form='8-K', stamp='2026-09-26T14:00:00Z', summary='Item 4.02 and Item 2.02', **extra):
    return dict(n=n, form=form, stamp=stamp, summary=summary, **extra)


def feed(rows):
    entries = []
    for row in rows:
        acc = row.get('accession', '0000123456-26-' + str(row['n']).zfill(6))
        title = row.get('title', row['form'] + ' - Example & Co (0000123456) (Filer)')
        category = row.get('category', row['form'])
        entries.append('<entry><id>' + escape(row.get('id', 'urn:tag:' + acc)) + '</id><title>' + escape(title)
                       + '</title><updated>' + escape(row['stamp']) + '</updated><category term=' + quoteattr(category)
                       + '/><link href=' + quoteattr(row.get('url', 'https://www.sec.gov/Archives/edgar/data/123456/' + acc + '-index.htm'))
                       + '/><summary type="html">' + escape(row['summary']) + '</summary><extra>whole extra field</extra></entry>')
    return ('<?xml version="1.0" encoding="UTF-8"?><feed xmlns="http://www.w3.org/2005/Atom"><updated>'
            + AT + '</updated>' + ''.join(entries) + '</feed>').encode('utf-8')


def build(rows, recipe=None, prior=None, overrides=None):
    recipe = deepcopy(recipe or RECIPE); originals = {}
    def retain(raw):
        ref = {'key': store.PRIVATE + model.sha(raw) + '.bin', 'sha256': model.sha(raw), 'bytes': len(raw)}
        originals[ref['sha256']] = raw
        return ref
    attempts = []
    for i, form in enumerate(model.FORMS[recipe['kind']]):
        attempts.append({'requested_form': form, 'url': model.feed_url(form), 'requested_at': AT,
                         'received_at': AT, 'status': 'http_response', 'http_status': 200,
                         'original': retain(feed(rows if i == 0 else []))})
    inputs = {'recipe': recipe, 'started_at': AT, 'generated_at': AT, 'attempts': attempts,
              'prior': {'key': model.HEADS[recipe['kind']], 'original': retain(model.encode(prior))} if prior is not None else None}
    if overrides: inputs.update(overrides)
    return model.build(inputs, lambda ref: originals[ref['sha256']])


class Error(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self, kind='8k'):
        self.head = model.HEADS[kind]
        self.data = {self.head: model.encode({'generated_at': '2026-09-26T15:00:00Z', 'filings': [], 'unknown': [0, False, None]})}
        self.reads = []; self.writes = []; self.denied = set(); self.truncated = set(); self.race = None; self.corrupt = False
    def get_object(self, **kw):
        key = kw['Key']; self.reads.append(key)
        if key in self.denied: raise Error('AccessDenied')
        if key not in self.data: raise Error('NoSuchKey')
        raw = self.data[key]
        if self.corrupt and key.startswith(store.PRIVATE): raw += b'!'
        return {'Body': BytesIO(raw), 'ContentLength': len(raw) + int(key in self.truncated), 'ETag': model.sha(raw)}
    def put_object(self, **kw):
        key = kw['Key']; raw = kw['Body']
        if self.race: self.race(key)
        if key in self.denied: raise Error('AccessDenied')
        old = self.data.get(key)
        if kw.get('IfNoneMatch') == '*' and old is not None: raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or model.sha(old) != kw['IfMatch']): raise Error('PreconditionFailed')
        self.data[key] = raw; self.writes.append(key)


class Response(BytesIO):
    def __init__(self, raw, url, code=200, headers=None):
        super().__init__(raw); self.url = url; self.code = code
        self.headers = {'Content-Length': str(len(raw)), **(headers or {})}
    def geturl(self): return self.url
    def getcode(self): return self.code


class Tests(unittest.TestCase):
    KIND = '8k'

    @classmethod
    def setUpClass(cls):
        cls.native_path = ROOT / 'aws/lambdas' / ('justhodl-sec-' + cls.KIND) / 'source/lambda_function.py'
        fake = ModuleType('boto3'); fake.client = lambda *a, **kw: None
        spec = importlib.util.spec_from_file_location('sec_atom_native_' + cls.KIND, cls.native_path)
        cls.native = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'boto3': fake}), patch.dict('os.environ', {}, clear=True):
            spec.loader.exec_module(cls.native)

    def setUp(self):
        self.memory = Memory(self.KIND); self.calls = []; self.sleeps = []
        self.recipe = deepcopy(RECIPE) if self.KIND == '8k' else {'kind': '10kq', 'window_days': 30}
        self.rows = [entry(form=model.FORMS[self.KIND][0])]

    def source(self, req, timeout=None):
        self.calls.append(req.full_url)
        self.assertEqual(req.get_header('Accept-encoding'), 'identity')
        self.assertGreater(timeout, 0)
        return Response(feed(self.rows), req.full_url)

    def publish(self, **kwargs):
        return store.run(self.memory, 'justhodl-dashboard-live', self.native_path, self.recipe,
                         'Existing Research Contact', now=lambda: AT, opener=kwargs.pop('opener', self.source),
                         sleep=self.sleeps.append, **kwargs)

    def packet(self): return model.strict(self.memory.data[self.memory.head])

    def test_actual_returned_amendment_never_requested_query_substitution(self):
        out = build([entry(form='10-K/A')], {'kind': '10kq', 'window_days': 30})
        self.assertEqual(out['filings'][0]['form'], '10-K/A')
        self.assertEqual(out['stats']['total_10k'], 0)
        self.assertEqual(out['stats']['total_10k_amended'], 1)
        self.assertEqual(out['filings'][0]['source_evidence'][0]['requested_form'], '10-K')
        self.assertFalse(out['calls_eligible']); self.assertIsNone(out['call'])

    def test_bad_future_naive_and_old_dates_never_become_current(self):
        rows = [entry(i, stamp=s) for i, s in enumerate(('bad', '2026-09-27T17:00:00Z', '2026-09-25',
                    '2026-09-26T14:00:00', '2026-02-30T12:00:00Z', '2025-01-01T12:00:00Z'), 1)]
        out = build(rows)
        self.assertEqual(out['filings'], [])
        self.assertEqual(len(out['excluded_records']), 6)
        self.assertEqual(out['stats']['last_24h'], 0)

    def test_identity_conflicts_and_missing_accessions_cannot_vote_or_join(self):
        out = build([entry(1, summary='Version A'), entry(1, summary='Version B'),
                     entry(2, accession='', id='missing', url='https://www.sec.gov/Archives/a'),
                     entry(3, accession='', id='missing', url='https://www.sec.gov/Archives/b')])
        self.assertEqual(len(out['filings']), 0)
        self.assertEqual(len(out['filing_versions']), 4)
        self.assertEqual(len(out['identity_conflicts']), 1)
        self.assertEqual(len(out['excluded_records']), 2)
        second = build([], prior=out)
        self.assertEqual(second['filings'], [])
        self.assertEqual(len(second['identity_conflicts']), 1)

    def test_returned_form_conflict_stays_excluded_after_retention(self):
        out = build([entry(category='10-K')])
        self.assertEqual(out['filings'], [])
        second = build([], prior=out)
        self.assertEqual(second['filings'], [])
        self.assertEqual(second['excluded_records'][0]['reason'], 'off_scope_or_unknown_form')

    def test_selected_current_version_survives_leaving_latest_snapshot(self):
        prior = {'filings': [{'company': 'Prior', 'accession': '0000123456-26-000001',
                              'filed_at': '2026-09-26T14:00:00Z', 'items': [], 'unknown_old': False}]}
        first = build([entry()], prior=prior)
        self.assertEqual(len(first['filing_versions']), 2)
        second = build([], prior=first)
        third = build([], prior=second)
        self.assertEqual(first['filings'][0]['version_id'], second['filings'][0]['version_id'])
        self.assertEqual(second['filings'][0]['version_id'], third['filings'][0]['version_id'])
        self.assertEqual(len(third['filing_versions']), 2)
        self.assertTrue(all(len(r['source_evidence']) == 1 for r in third['filing_versions']))

    def test_no_row_summary_or_classification_caps_and_unknown_fields_retained(self):
        text = 'Item 4.02 Item 2.02 ' + 'Long original & value ' * 400
        out = build([entry(i, summary=text) for i in range(1, 602)], prior={'filings': [], 'extra': [False, 0, None]})
        self.assertEqual(len(out['filings']), 601)
        self.assertEqual(len(out['red_flags']), 601)
        self.assertEqual(len(out['high_impact']), 601)
        self.assertEqual(out['stats']['total_filings'], 601)
        self.assertEqual(out['filings'][0]['summary'], text)
        self.assertIn('whole extra field', out['filings'][0]['atom_entry_xml'])
        self.assertEqual(out['extra'], [False, 0, None])
        amended = build([entry(i, form='10-Q/A') for i in range(1, 350)], {'kind': '10kq', 'window_days': 30})
        self.assertEqual(len(amended['amended']), 349)

    def test_invalid_legacy_records_are_preserved_without_unhashable_item_crash(self):
        prior = {'filings': [False, None, ['unusual'], {'accession': '0000123456-26-000002',
                            'filed_at': '2026-09-26T12:00:00Z', 'items': [False, [], {}, '4.02']}]}
        out = build([], prior=prior)
        self.assertEqual(len(out['malformed_prior_records']), 3)
        self.assertEqual(out['by_item_counts']['4.02'], 1)
        self.assertEqual(out['filings'][0]['items'], [False, [], {}, '4.02'])

    def test_strict_json_xml_dates_and_exact_family_boundaries(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'{"a":1e-999}', b'\xff'):
            with self.subTest(raw=raw), self.assertRaises((ValueError, UnicodeError)): model.strict(raw)
        for raw in (b'<html/>', b'<!DOCTYPE feed><feed/>', b'<feed>'):
            with self.subTest(raw=raw), self.assertRaises(Exception): model.parse_atom(raw)
        for invalid in (False, '', '2026-09-27', '2026-09-27T24:10:00Z'):
            self.assertIsNone(model.clock(invalid))
        with self.assertRaises(ValueError): build([], prior={'contract': 'foreign', 'filings': []})
        with self.assertRaises(ValueError): build([], overrides={'generated_at': '2026-09-26T16:00:00Z'})

    def test_xml_namespace_process_state_cannot_change_record_identity(self):
        first = build([entry()])
        with patch.dict(model.ET._namespace_map, {'http://www.w3.org/2005/Atom': 'different'}):
            second = build([entry()])
        self.assertEqual(first, second)

    def test_complete_publication_and_exact_original_replay_without_provider_calls(self):
        old = self.memory.data[self.memory.head]
        self.assertEqual(self.publish()['statusCode'], 200)
        packet = self.packet()
        self.assertIn(old, self.memory.data.values())
        self.assertEqual(len(self.calls), len(model.FORMS[self.KIND]))
        self.assertEqual(packet['quality']['status'], 'partial')
        self.assertEqual(packet['quality']['ingestion_status'], 'all_requested_responses_parsed')
        self.assertFalse(packet['original_vintage_verified'])
        writes = list(self.memory.writes); calls = list(self.calls)
        with patch('urllib.request.build_opener', side_effect=AssertionError('No provider in replay')):
            result = store.replay(self.memory, 'justhodl-dashboard-live', self.native_path, self.KIND, packet)
        self.assertEqual(result['status'], 'complete_sec_atom_originals_replayed')
        self.assertEqual(self.memory.writes, writes); self.assertEqual(self.calls, calls)
        self.assertTrue(all(store.allowed(key, self.KIND) for key in self.memory.reads + self.memory.writes))
        for name, expected in packet['compiler_sha256'].items():
            self.assertEqual(expected, model.sha(store.compiler_bytes(self.native_path)[name]))

    def test_native_entry_has_exact_recipe_and_http_does_nothing(self):
        with patch.object(self.native.boto3, 'client', return_value=self.memory) as client, patch.object(store, 'run', return_value={'ok': True}) as run:
            self.assertEqual(self.native.lambda_handler({'requestContext': {}}, None)['statusCode'], 409)
            client.assert_not_called(); run.assert_not_called()
            self.assertEqual(self.native.lambda_handler({}, None), {'ok': True})
            self.assertEqual(run.call_args.args[3]['kind'], self.KIND)
            self.assertEqual(run.call_args.args[3]['window_days'], self.native.WINDOW_DAYS)
            self.assertEqual(run.call_args.args[2], str(self.native_path))
            run.side_effect = ValueError('Secret error must not enter public body')
            failure = self.native.lambda_handler({}, None)
            self.assertEqual(failure['statusCode'], 503)
            self.assertNotIn('Secret', failure['body'])
        self.assertEqual(store.run(None, '', None, None, None, event={'httpMethod': 'GET'})['statusCode'], 409)

    def test_all_feeds_fail_without_refreshing_public_head(self):
        before = self.memory.data[self.memory.head]
        def bad(req, timeout=None): self.calls.append(req.full_url); return Response(b'upstream error', req.full_url, 503)
        with self.assertRaises(ValueError): self.publish(opener=bad)
        self.assertEqual(self.memory.data[self.memory.head], before)
        self.assertEqual(len(self.calls), len(model.FORMS[self.KIND]))
        self.assertIn(b'upstream error', self.memory.data.values())
        self.assertTrue(all(key.startswith(store.PRIVATE) for key in self.memory.writes))

    def test_partial_feeds_report_failure_and_rate_limit_stops_remaining(self):
        recipe = {'kind': '10kq', 'window_days': 30}; memory = Memory('10kq'); calls = []
        def source(req, timeout=None):
            calls.append(req.full_url)
            return Response(feed([entry(form='10-K')]) if len(calls) == 1 else b'rate limited', req.full_url, 200 if len(calls) == 1 else 429)
        store.run(memory, 'justhodl-dashboard-live', self.native_path, recipe, 'Existing Research', now=lambda: AT, opener=source, sleep=lambda _: None)
        packet = model.strict(memory.data[memory.head])
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(packet['source_responses']), 4)
        self.assertEqual(packet['source_responses'][-1]['status'], 'rate_limit_not_attempted')
        self.assertEqual(packet['quality']['ingestion_status'], 'partial_source_responses')
        self.assertEqual(len(packet['stats']['fetch_errors']), 3)

    def test_malformed_feed_is_not_an_empty_success_or_fresh_date(self):
        before = self.memory.data[self.memory.head]
        with self.assertRaises(ValueError): self.publish(opener=lambda req, timeout=None: Response(b'<broken', req.full_url))
        self.assertEqual(self.memory.data[self.memory.head], before)
        self.assertIn(b'<broken', self.memory.data.values())

    def test_content_length_encoding_redirect_and_population_bounds_refuse_publish(self):
        for label, response in (
            ('length', lambda req: Response(b'short', req.full_url, headers={'Content-Length': '900'})),
            ('encoding', lambda req: Response(b'compressed', req.full_url, headers={'Content-Encoding': 'gzip'})),
            ('redirect', lambda req: Response(b'moved', 'https://different.example/'))):
            with self.subTest(label=label):
                self.memory = Memory(self.KIND); before = self.memory.data[self.memory.head]
                with self.assertRaises(ValueError): self.publish(opener=lambda req, timeout=None: response(req))
                self.assertEqual(self.memory.data[self.memory.head], before)
        with patch.object(store, 'LIMIT', 3), self.assertRaises(ValueError): store.whole(BytesIO(b'1234'))
        with self.assertRaises(ValueError): store.whole(BytesIO(b'whole'), deadline=0)

    def test_prior_corrupt_or_denied_and_private_capture_failure_prevent_refresh(self):
        for raw in (b'{bad', b'{"filings":false}', b'{"filings":[],"filings":[]}'):
            self.memory = Memory(self.KIND); self.memory.data[self.memory.head] = raw
            with self.assertRaises(ValueError): self.publish()
            self.assertEqual(self.memory.data[self.memory.head], raw)
            self.assertIn(raw, self.memory.data.values())
        self.memory = Memory(self.KIND); self.memory.denied.add(self.memory.head)
        with self.assertRaises(Error): self.publish()
        self.assertEqual(self.memory.writes, [])
        self.memory = Memory(self.KIND); self.memory.corrupt = True; before = self.memory.data[self.memory.head]
        with self.assertRaises(ValueError): self.publish()
        self.assertEqual(self.memory.data[self.memory.head], before)

    def test_concurrent_writer_never_overwritten_or_rolled_back(self):
        other = model.encode({'filings': [], 'generated_at': AT, 'other_writer': True})
        self.memory.race = lambda key: self.memory.data.update({key: other}) if key == self.memory.head else None
        with self.assertRaises(Error): self.publish()
        self.assertEqual(self.memory.data[self.memory.head], other)
        self.assertNotIn(self.memory.head, self.memory.writes)

    def test_replay_detects_tampered_packet_original_or_compiler(self):
        self.publish(); packet = self.packet()
        changed = deepcopy(packet); changed['stats']['total' if self.KIND == '10kq' else 'total_filings'] = 999
        with self.assertRaises(ValueError): store.replay(self.memory, 'justhodl-dashboard-live', self.native_path, self.KIND, changed)
        with patch.object(store, 'compiler_bytes', return_value={name: b'changed' for name in store.COMPILERS}), self.assertRaises(ValueError):
            store.replay(self.memory, 'justhodl-dashboard-live', self.native_path, self.KIND, packet)
        self.memory.corrupt = True
        with self.assertRaises(ValueError): store.replay(self.memory, 'justhodl-dashboard-live', self.native_path, self.KIND, packet)

    def test_read_boundary_budget_and_native_recipe_are_enforced_before_acquisition(self):
        for key in ('data/pm-decision.json', 'learning/morning_run_log.json', 'private/account.json', 'data/sec-filings-intel.json'):
            with self.assertRaises(ValueError): store.get(self.memory, 'justhodl-dashboard-live', key, self.KIND)
        self.assertEqual(self.memory.reads, [])
        with patch.object(store.time, 'monotonic', side_effect=[0, 299]), self.assertRaises(ValueError): self.publish()
        self.assertEqual(self.calls, [])
        with patch.object(self.native, 'S3_KEY', 'data/other.json'), patch.object(self.native.boto3, 'client') as client:
            self.assertEqual(self.native.lambda_handler({}, None)['statusCode'], 503)
            client.assert_not_called()


def run(kind):
    Tests.KIND = kind
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    return 0 if result.wasSuccessful() else 1
