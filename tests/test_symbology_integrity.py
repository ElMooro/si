"""Whole invented state/provider regressions; never inspect real engine data."""
from pathlib import Path
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import patch
import importlib.util
import io
import json
import sys
import unittest
import urllib.error

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[0] / 'si-batch-improvements' if (HERE / 'openfigi.py').exists() else HERE.parents[0]
CANDIDATE = HERE if (HERE / 'openfigi.py').exists() else ROOT / 'aws/lambdas/justhodl-symbology-master/source'
SHARED = HERE / 'openfigi.py' if (HERE / 'openfigi.py').exists() else ROOT / 'aws/shared/openfigi.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    obj = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'boto3': SimpleNamespace(client=lambda *a, **k: SimpleNamespace()),
                                 'raw_snapshot': SimpleNamespace(snapshot=None)}):
        if spec is None:
            raise AssertionError('Python source path required')
        spec.loader.exec_module(obj)
    return obj


figi = load('tested_openfigi', SHARED)
bonds = load('tested_bonds', CANDIDATE / 'bond_symbology.py')
native = load('tested_symbology', CANDIDATE / 'lambda_function.py')
NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)
CUSIP = '123456780'
OTHER = '876543210'
SECURITY = {'figi': 'BBG00TEST001', 'ticker': 'EXAMPLE', 'name': 'Invented issuer',
            'securityType': 'Corporate Bond', 'marketSector': 'Corp', 'unknown_provider_field': ['retained', None]}


class StorageError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class FakeS3:
    def __init__(self, queue=None, prior=None):
        self.docs = {bonds.QUEUE_KEY: queue if queue is not None else [CUSIP]}
        if prior is not None:
            self.docs[bonds.MASTER_KEY] = prior
        self.puts = []
        self.failures = {}
        self.streams = []
        self.put_error = None
        self.response_patch = {}
    def get_object(self, **kw):
        key = kw['Key']
        if key in self.failures:
            raise self.failures[key]
        if key not in self.docs:
            raise StorageError('NoSuchKey')
        raw = json.dumps(self.docs[key]).encode()
        body = io.BytesIO(raw)
        self.streams.append(body)
        return {**{'Body': body, 'ContentLength': len(raw), 'ETag': '"whole-prior-version"'}, **self.response_patch}
    def put_object(self, **kw):
        self.puts.append(kw)
        if self.put_error:
            raise self.put_error
    def written(self):
        return json.loads(self.puts[-1]['Body'])


def answer(cusip=CUSIP, item=None):
    item = {'data': [deepcopy(SECURITY)]} if item is None else item
    return {**figi.resolution(item), 'query': {'idType': 'ID_CUSIP', 'idValue': cusip}}


def run(client, resolve=None, limit=100):
    with patch.object(bonds.time, 'monotonic', return_value=100):
        return bonds.enrich(client, 'invented-bucket', resolve or (lambda c, **kw: answer(c)),
                            deadline=140, limit=limit, now=NOW)


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.previous = figi._NEXT_MAPPING_AT
        figi._NEXT_MAPPING_AT = 0
    def tearDown(self):
        figi._NEXT_MAPPING_AT = self.previous
    def test_cardinality_failure_does_not_shift_later_job_identity(self):
        for response in ([{'data': [SECURITY]}], [{}, {}, {}], {}, [None, {}]):
            with patch.object(figi, '_post', return_value=response), patch.object(figi.time, 'monotonic', return_value=100), patch.object(figi.time, 'sleep'):
                rows = figi.mapping([{'idValue': 'one'}, {'idValue': 'two'}], api_key='invented')
            self.assertEqual(len(rows), 2)
            self.assertTrue(all('error' in row for row in rows))
    def test_only_explicit_warning_is_a_negative_match(self):
        self.assertEqual(figi.resolution({'warning': 'No identifier found.'})['status'], 'no_match')
        for item in (None, {}, {'data': []}, {'error': 'Unavailable'}, {'warning': ''}, {'warning': 'none', 'data': [SECURITY]}, {'data': [{'figi': ''}]}):
            self.assertEqual(figi.resolution(item)['status'], 'error')
    def test_all_ambiguous_candidates_and_extra_fields_are_retained(self):
        item = {'data': [deepcopy(SECURITY), {**SECURITY, 'figi': 'BBG00TEST002'}], 'extra': [1, None]}
        result = figi.resolution(item)
        self.assertEqual(result['status'], 'ambiguous')
        self.assertEqual(result['response'], item)
        self.assertNotIn('security', result)
        self.assertEqual(figi.resolution({'data': [SECURITY]})['security'], SECURITY)
    def test_compatibility_helper_refuses_ambiguous_identity(self):
        with patch.object(figi, 'resolve_cusip', return_value=answer(item={'data': [SECURITY, SECURITY]})):
            self.assertIsNone(figi.cusip_to_security(CUSIP))
    def test_request_failure_is_not_no_match(self):
        with patch.object(figi, 'mapping', return_value=None):
            result = figi.resolve_cusip(CUSIP)
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['query']['idValue'], CUSIP)
    def test_deadline_blocks_credentials_and_network(self):
        with patch.object(figi.time, 'monotonic', return_value=100), patch.object(figi, 'get_api_key') as key, patch.object(figi, '_post') as post:
            result = figi.mapping([{}], deadline=99)
        self.assertEqual(result, [{'error': 'deadline exhausted'}])
        key.assert_not_called(); post.assert_not_called()
    def test_failed_calls_are_still_paced_and_deadline_defers(self):
        clock = [100.0]
        def sleep(seconds): clock[0] += seconds
        with patch.object(figi.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(figi.time, 'sleep', side_effect=sleep), patch.object(figi, '_post', return_value=None) as post:
            figi.mapping([{}], api_key='invented', deadline=105)
            result = figi.mapping([{}], api_key='invented', deadline=100.2)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(result[0]['error'], 'deadline exhausted')
    def test_retry_after_is_never_shortened(self):
        for wait in ('3600', 'nan', '-1', 'not-a-number', 'Wed, 01 Oct 2026 00:00:00 GMT'):
            exc = urllib.error.HTTPError(figi.MAPPING_URL, 429, 'throttled', {'Retry-After': wait}, io.BytesIO(b'provider failure'))
            with patch.object(figi.urllib.request, 'urlopen', side_effect=exc) as url, patch.object(figi.time, 'sleep') as sleep:
                self.assertIsNone(figi._post(figi.MAPPING_URL, [{}], None))
            self.assertEqual(url.call_count, 1); sleep.assert_not_called()
            self.assertTrue(exc.fp.closed)
    def test_http_whole_length_duplicate_and_nonfinite_validation(self):
        for raw, headers, status in ((b'[]', {'Content-Length': '10'}, 200),
                                     (b'[]', {'Content-Range': 'bytes 0-1/5'}, 200),
                                     (b'[]', {}, 206),
                                     (b'[{"data":[],"data":[]}]', {}, 200),
                                     (b'[{"bad":NaN}]', {}, 200)):
            stream = io.BytesIO(raw); stream.headers = headers; stream.status = status
            with patch.object(figi.urllib.request, 'urlopen', return_value=stream):
                self.assertIsNone(figi._post(figi.MAPPING_URL, [{}], None))
            self.assertTrue(stream.closed)
    def test_complete_fragmented_provider_body_is_retained(self):
        raw = json.dumps([{'data': [SECURITY]}]).encode()
        class Fragments(io.BytesIO):
            def read1(self, n): return super().read(min(n, 3))
        stream = Fragments(raw); stream.headers = {'Content-Length': str(len(raw))}; stream.status = 200
        with patch.object(figi.urllib.request, 'urlopen', return_value=stream):
            self.assertEqual(figi._post(figi.MAPPING_URL, [{}], None), [{'data': [SECURITY]}])
        self.assertTrue(stream.closed)
    def test_duplicate_candidate_rows_are_not_independent_confirmation(self):
        self.assertEqual(figi.resolution({'data': [SECURITY, SECURITY]})['status'], 'ambiguous')


class StorageTests(unittest.TestCase):
    def test_provider_failure_remains_retryable_and_never_negative(self):
        client = FakeS3()
        stats = run(client, lambda c, **kw: answer(c, {'error': 'temporary failure'}))
        row = client.written()['by_cusip'][CUSIP]
        self.assertFalse(row['no_match']); self.assertFalse(row['resolution_eligible'])
        self.assertEqual(stats['remaining'], 1)
        self.assertEqual(row['resolution']['status'], 'error')
    def test_prior_timeout_access_denied_and_malformed_preserve_existing(self):
        for exc in (TimeoutError(), StorageError('AccessDenied')):
            client = FakeS3(); client.failures[bonds.MASTER_KEY] = exc
            result = run(client)
            self.assertEqual(result['reason'], 'prior_read_or_schema_failed')
            self.assertFalse(client.puts)
        for prior in (None, [], {}, {'by_cusip': None}, {'by_cusip': {CUSIP: []}}):
            client = FakeS3(); client.docs[bonds.MASTER_KEY] = prior
            result = run(client)
            self.assertEqual(result['reason'], 'prior_read_or_schema_failed')
            self.assertFalse(client.puts)
    def test_only_missing_master_permits_conditional_create(self):
        client = FakeS3()
        self.assertTrue(run(client)['published'])
        self.assertEqual(client.puts[0]['IfNoneMatch'], '*')
        self.assertNotIn('IfMatch', client.puts[0])
    def test_whole_unrelated_rows_and_top_level_fields_survive(self):
        prior = {'schema_version': '1.0', 'custom': {'all': ['retained', None]}, 'by_cusip': {OTHER: {'figi': 'BBG000PRIOR01', 'all': [1, 2, 3]}}}
        client = FakeS3(prior=prior)
        result = run(client)
        self.assertTrue(result['published'])
        self.assertEqual(client.written()['custom'], prior['custom'])
        self.assertEqual(client.written()['by_cusip'][OTHER], prior['by_cusip'][OTHER])
        self.assertEqual(client.puts[0]['IfMatch'], '"whole-prior-version"')
        self.assertEqual(client.written()['by_cusip'][CUSIP]['resolution']['response']['data'][0], SECURITY)
    def test_concurrent_writer_and_put_failure_never_retry_unconditionally(self):
        for code in ('PreconditionFailed', 'ConditionalRequestConflict', 'AccessDenied'):
            client = FakeS3(prior={'by_cusip': {OTHER: {'figi': 'BBG000PRIOR01'}}}); client.put_error = StorageError(code)
            result = run(client)
            self.assertFalse(result['published']); self.assertEqual(len(client.puts), 1)
            self.assertIn('IfMatch', client.puts[0])
    def test_truncated_wrong_length_unversioned_duplicate_nonfinite_json_close(self):
        for raw, extra in ((b'{"by_cusip":{}}', {'ContentLength': 99}),
                           (b'{"by_cusip":{}}', {'ETag': None}),
                           (b'{"by_cusip":{},"by_cusip":{}}', {}),
                           (b'{"by_cusip":{},"bad":NaN}', {})):
            body = io.BytesIO(raw)
            client = SimpleNamespace(get_object=lambda **kw: {'Body': body, 'ContentLength': len(raw), 'ETag': '"v"', **extra})
            with self.assertRaises(ValueError): bonds._read(client, 'invented', bonds.MASTER_KEY)
            self.assertTrue(body.closed)
    def test_legacy_negative_is_retried_and_retained_explicitly(self):
        old = {'no_match': True, 'vendor_tag': 'old'}
        client = FakeS3(prior={'by_cusip': {CUSIP: old}})
        result = run(client, lambda c, **kw: answer(c, {'error': 'temporary failure'}))
        row = client.written()['by_cusip'][CUSIP]
        self.assertEqual(result['attempted'], 1); self.assertEqual(row['legacy_record'], old)
        self.assertFalse(row['no_match'])
    def test_unattempted_queue_work_precedes_repeated_failure(self):
        old = {'resolution': answer(CUSIP, {'error': 'failure'}), 'last_attempt_at': NOW.isoformat()}
        client = FakeS3(queue=[CUSIP, OTHER, OTHER, 42], prior={'by_cusip': {CUSIP: old}})
        seen = []
        def resolve(c, **kw): seen.append(c); return answer(c)
        result = run(client, resolve, limit=1)
        self.assertEqual(seen, [OTHER]); self.assertEqual(result['invalid_queue_rows'], 1)
        self.assertEqual(result['remaining'], 1)
    def test_valid_negative_expires_and_ambiguity_does_not_finish_queue(self):
        row = {'resolution': answer(item={'warning': 'No identifier found.'}), 'last_attempt_at': NOW.isoformat()}
        self.assertTrue(bonds._qualified(row, NOW, CUSIP))
        self.assertFalse(bonds._qualified(row, NOW + timedelta(days=7), CUSIP))
        self.assertFalse(bonds._qualified(row, NOW - timedelta(seconds=1), CUSIP))
        client = FakeS3(); result = run(client, lambda c, **kw: answer(c, {'data': [SECURITY, SECURITY]}))
        self.assertEqual(result['ambiguous'], 1); self.assertEqual(result['remaining'], 1)
        self.assertNotIn('figi', client.written()['by_cusip'][CUSIP])
    def test_deadline_defers_before_any_storage_or_provider_call(self):
        client = SimpleNamespace(get_object=lambda **kw: self.fail('storage read despite no time'))
        with patch.object(bonds.time, 'monotonic', return_value=100):
            result = bonds.enrich(client, 'invented', lambda *a, **k: self.fail('provider request'), deadline=105)
        self.assertEqual(result['reason'], 'insufficient_time')
    def test_false_negative_or_foreign_prior_result_never_qualifies(self):
        for result in ({'schema': bonds.SCHEMA, 'status': 'no_match', 'query': {'idType': 'ID_CUSIP', 'idValue': CUSIP}},
                       answer(OTHER, {'warning': 'No identifier found.'}),
                       {**answer(CUSIP, {'error': 'failure'}), 'status': 'no_match'}):
            self.assertFalse(bonds._qualified({'resolution': result, 'last_attempt_at': NOW.isoformat()}, NOW, CUSIP))
    def test_wrong_identifier_or_untyped_resolver_is_error(self):
        for result in (None, answer(OTHER), {'status': 'resolved'}):
            client = FakeS3(); stats = run(client, lambda c, **kw: result)
            self.assertEqual(stats['errors'], 1)
            self.assertFalse(client.written()['by_cusip'][CUSIP]['no_match'])


class NativeTests(unittest.TestCase):
    def test_missing_low_and_invalid_native_clock_defer_without_storage(self):
        with patch.dict(sys.modules, {'openfigi': figi, 'bond_symbology': bonds}), patch.object(native.boto3, 'client') as client:
            for context in (None, SimpleNamespace(get_remaining_time_in_millis=lambda: 15000), SimpleNamespace(get_remaining_time_in_millis=lambda: float('nan'))):
                self.assertEqual(native.enrich_bond_cusips(context=context)['status'], 'deferred')
            client.assert_not_called()
    def test_deferred_paths_preserve_legacy_count_fields_without_false_remaining_zero(self):
        with patch.dict(sys.modules, {'openfigi': figi, 'bond_symbology': bonds}):
            for context in (None, SimpleNamespace(get_remaining_time_in_millis=lambda: 1000)):
                result = native.enrich_bond_cusips(context=context)
                self.assertEqual({k: result[k] for k in ('resolved', 'no_match', 'errors', 'remaining')},
                                 {'resolved': 0, 'no_match': 0, 'errors': 0, 'remaining': None})
    def test_optional_failure_does_not_prevent_equity_write(self):
        storage = FakeS3(); raw = json.dumps({'0': {'ticker': 'TEST', 'cik_str': 123, 'title': 'Invented issuer'}}).encode()
        response = io.BytesIO(raw)
        with ExitStack() as stack:
            stack.enter_context(patch.object(native, 's3', storage))
            stack.enter_context(patch.object(native, 'snapshot', None))
            stack.enter_context(patch.object(native.urllib.request, 'urlopen', return_value=response))
            stack.enter_context(patch.object(native, 'enrich_figi', return_value={}))
            stack.enter_context(patch.object(native, 'enrich_cusip_chain', return_value={}))
            stack.enter_context(patch.dict(sys.modules, {'openfigi': figi, 'bond_symbology': SimpleNamespace(enrich=lambda *a, **kw: (_ for _ in ()).throw(RuntimeError()))}))
            result = native.lambda_handler({}, SimpleNamespace(get_remaining_time_in_millis=lambda: 60000))
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(storage.puts[0]['Key'], 'data/symbology/master.json')
        packet = storage.written()
        self.assertEqual(packet['by_ticker']['TEST']['cik'], '0000000123')
        self.assertEqual(packet['enrichment_status']['bond_cusips']['reason'], 'optional_enrichment_failed')


class PredecessorTests(unittest.TestCase):
    def source(self, filename):
        p = HERE / ('predecessor-' + filename + '.py.txt') if HERE == CANDIDATE else ROOT / 'tests/fixtures/symbology' / ('predecessor-' + filename + '.py.txt')
        namespace = {'__name__': 'whole_retained_predecessor'}
        with patch.dict(sys.modules, {'boto3': SimpleNamespace(client=lambda *a, **kw: SimpleNamespace()), 'raw_snapshot': SimpleNamespace(snapshot=None)}):
            exec(compile(p.read_bytes(), str(p), 'exec'), namespace)
        return namespace
    def test_whole_predecessor_reproduces_three_repaired_failures(self):
        old = self.source('lambda')
        fake = SimpleNamespace(cusip_to_security=lambda *a: None)
        client = FakeS3(); old['s3'] = client
        with patch.dict(sys.modules, {'openfigi': fake}), patch.object(old['time'], 'sleep'):
            old['enrich_bond_cusips']()
        self.assertEqual(client.written()['by_cusip'][CUSIP], {'no_match': True})
        client = FakeS3(prior={'by_cusip': {OTHER: {'figi': 'BBG00PRIOR01'}}}); client.failures[bonds.MASTER_KEY] = TimeoutError(); old['s3'] = client
        with patch.dict(sys.modules, {'openfigi': fake}), patch.object(old['time'], 'sleep'):
            old['enrich_bond_cusips']()
        self.assertNotIn(OTHER, client.written()['by_cusip'])
        shared = self.source('openfigi'); shared['_post'] = lambda *a: [{'data': [SECURITY]}]
        with patch.object(shared['time'], 'sleep'):
            self.assertEqual(len(shared['mapping']([{}, {}], api_key='invented')), 1)


if __name__ == '__main__':
    unittest.main()
