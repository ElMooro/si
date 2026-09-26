"""Original-byte capture/replay, partial transports and authority boundaries."""
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
import json
import sys
import unittest
import urllib.error
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'source'))
import sovereign_sources as src
import sovereign_history as pub

NOW = '2026-09-26T23:00:00+00:00'
COUNTRIES = {'Example': ('example', 'Test')}
PAGE = b'<html><script>var jsGlobalVars = {"name":"quoted } brace", "id":1};</script></html>'
QUOTE = pub.encode({'success': True, 'bond10y': '3.25', 'lastCds': 0, 'mainSpreadValue': '----',
                    'lastDataValDesc': 'provider date', 'lastRatingValue': 'AA', 'cbRateNumber': 0})


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self):
        self.rows = {}
        self.writes = []
        self.fail = False

    def put_object(self, Bucket, Key, Body, **kw):
        if self.fail:
            raise Error('AccessDenied')
        if Key in self.rows:
            raise Error('PreconditionFailed')
        assert Key.startswith(pub.PRIVATE) and kw['IfNoneMatch'] == '*'
        self.rows[Key] = Body
        self.writes.append(Key)

    def get_object(self, Bucket, Key):
        return {'Body': BytesIO(self.rows[Key])}


class Response(BytesIO):
    def __init__(self, raw, url, status=200, headers=None, chunk=9):
        super().__init__(raw)
        self.url, self.status, self.chunk = url, status, chunk
        self.headers = {'Content-Length': str(len(raw)), **(headers or {})}

    def read(self, n=-1):
        return super().read(min(n, self.chunk))

    def getcode(self):
        return self.status

    def geturl(self):
        return self.url


def fixture(page=PAGE, quote=QUOTE, **options):
    m, calls, responses = Memory(), [], []
    def opener(request, timeout):
        calls.append(request)
        response = Response(page if request.data is None else quote, request.full_url, **options)
        responses.append(response)
        return response
    return src.Capture(m, 'b', COUNTRIES, 'test', opener=opener, now=lambda: NOW), m, calls, responses


class SourceTests(unittest.TestCase):
    def test_actual_handler_all_country_projections_bind_to_complete_originals(self):
        import run_tests as native
        m, _ = native.fixture()
        env = native.handler(m)
        env['sovereign_sources'] = src
        real_capture = src.Capture
        calls = []
        def opener(req, timeout):
            calls.append(req)
            return Response(PAGE if req.data is None else QUOTE, req.full_url, chunk=65536)
        def factory(*args):
            return real_capture(*args, opener=opener, now=lambda: native.NOW.isoformat())
        with patch.object(src, 'Capture', factory):
            env['lambda_handler']()
        packet = json.loads(m.rows[pub.HEAD])
        proof = src.verify_publication(packet, m.rows.__getitem__, env['COUNTRIES'])
        self.assertEqual(len(calls), 90)
        self.assertEqual(proof['direct_provider_fields_replayed'], 45 * 7)
        self.assertEqual(packet['n_countries'], 45)
        self.assertFalse(proof['model_qualified'])
        for mode in ('field', 'coverage', 'authority', 'clock', 'source_count'):
            changed = deepcopy(packet)
            if mode == 'field': changed['countries'][0]['cds_bp'] = 999
            if mode == 'coverage': changed['errors'] = ['Germany']
            if mode == 'authority': changed['calls_eligible'] = True
            if mode == 'clock': changed['generated_at'] = '2026-09-25T00:00:00Z'
            if mode == 'source_count': changed['source_evidence']['complete_responses_retained'] = 91
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                src.verify_publication(changed, m.rows.__getitem__, env['COUNTRIES'])

    def test_complete_originals_and_context_replay_preserve_zero_and_private_bytes(self):
        cap, m, calls, responses = fixture()
        result = cap.country('example')
        evidence = cap.finish()
        manifest = json.loads(m.rows[evidence['manifest']['key']])
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(r.closed for r in responses))
        self.assertIn(PAGE, m.rows.values())
        self.assertIn(QUOTE, m.rows.values())
        self.assertEqual(result['cds_bp'], 0)
        self.assertIsNone(result['spread_vs_bund_bp'])
        self.assertEqual(src.replay(manifest, m.rows.__getitem__), {'example': result})
        self.assertFalse(evidence['definitions_verified'])
        self.assertFalse(evidence['original_source_replay_verified'])
        self.assertNotIn('quoted } brace', json.dumps(evidence))
        self.assertTrue(all(k.startswith(pub.PRIVATE) for k in m.writes))

    def test_complete_failed_http_body_is_retained_without_a_quote(self):
        cap, m, calls, responses = fixture(page=b'upstream unavailable', status=503)
        self.assertIsNone(cap.country('example'))
        evidence = cap.finish()
        manifest = json.loads(m.rows[evidence['manifest']['key']])
        self.assertIn(b'upstream unavailable', m.rows.values())
        self.assertEqual(evidence['coverage'][0]['status'], 'page_unavailable')
        self.assertEqual(src.replay(manifest, m.rows.__getitem__), {})
        self.assertEqual(len(calls), 1)
        self.assertTrue(responses[0].closed)

    def test_partial_oversized_changed_route_and_encoding_never_parse_or_fake_retention(self):
        for opts in ({'headers': {'Content-Length': '100000'}}, {'headers': {'Content-Encoding': 'gzip'}}):
            cap, m, calls, responses = fixture(**opts)
            self.assertIsNone(cap.country('example'))
            cap.finish()
            self.assertEqual(len(calls), 1)
            self.assertTrue(responses[0].closed)
        cap, m, calls, responses = fixture()
        with patch.object(src, 'LIMIT', 12):
            self.assertIsNone(cap.country('example'))
        self.assertFalse(cap.requests[0]['complete_body_retained'])
        self.assertNotIn(PAGE[:13], m.rows.values())
        cap.finish()
        cap, m, calls, responses = fixture()
        original = cap.opener
        def redirect(*args, **kw):
            response = original(*args, **kw)
            response.url = 'https://other.example/'
            return response
        cap.opener = redirect
        self.assertIsNone(cap.country('example'))
        self.assertEqual(cap.requests[0]['status'], 'http_or_route_rejected')

    def test_denied_or_corrupt_evidence_is_fatal_and_not_a_missing_country(self):
        cap, m, calls, responses = fixture()
        m.fail = True
        with self.assertRaises(src.RetentionError):
            cap.country('example')
        self.assertEqual(len(calls), 1)
        self.assertTrue(responses[0].closed)
        cap, m, calls, responses = fixture()
        m.rows[pub.PRIVATE + pub.sha(PAGE) + '.bin'] = b'corrupt'
        with self.assertRaises(src.RetentionError):
            cap.country('example')

    def test_missing_duplicate_country_and_wrong_routes_cannot_finish(self):
        cap, m, calls, _ = fixture()
        with self.assertRaises(ValueError):
            cap.finish()
        with self.assertRaises(ValueError):
            cap.country('private')
        with self.assertRaises(ValueError):
            cap.request('example', 'page', b'wrong')
        self.assertEqual(calls, [])
        cap.country('example')
        with self.assertRaises(ValueError):
            cap.country('example')

    def test_invalid_context_json_and_typed_values_are_not_silently_reinterpreted(self):
        for page in (b'no globals', b'var jsGlobalVars = {"id":1,"id":2};', b'var jsGlobalVars = {"x":NaN};', b'var jsGlobalVars = {};', b'\xff'):
            cap, m, calls, _ = fixture(page=page)
            self.assertIsNone(cap.country('example'))
            self.assertEqual(len(calls), 1)
            self.assertEqual(cap.finish()['coverage'][0]['status'], 'request_context_invalid')
        for quote in (b'{"success":true,"bond10y":1,"bond10y":2}', b'{"success":true,"bond10y":NaN}', b'{"success":1,"bond10y":2}', b'[]', b'\xff'):
            cap, m, _, _ = fixture(quote=quote)
            self.assertIsNone(cap.country('example'))
            self.assertEqual(cap.finish()['coverage'][0]['status'], 'quote_invalid')
        row = src.project(pub.encode({'success': True, 'bond10y': True, 'lastCds': 'Infinity', 'lastRatingValue': {'html': 'no'}}))
        self.assertTrue(all(v is None for v in row.values()))

    def test_altered_bytes_context_compiler_mapping_identity_or_projection_fail_replay(self):
        cap, m, _, _ = fixture()
        cap.country('example')
        evidence = cap.finish()
        base = json.loads(m.rows[evidence['manifest']['key']])
        for mode in ('compiler', 'mapping', 'body', 'post', 'hash', 'coverage', 'orphan', 'identity'):
            doc = deepcopy(base)
            values = dict(m.rows)
            if mode == 'compiler': doc['compiler_sha256']['lambda_function.py'] = '0' * 64
            if mode == 'mapping': doc['field_mapping']['cds_bp'] = 'bond10y'
            if mode == 'body': values[doc['requests'][1]['response_body']['key']] = b'changed'
            if mode == 'post': doc['requests'][1]['request_body'] = doc['requests'][1]['response_body']
            if mode == 'hash': doc['coverage'][0]['projection_sha256'] = '0' * 64
            if mode == 'coverage': doc['coverage'][0]['slug'] = 'other'
            if mode == 'orphan': doc['requests'].append(doc['requests'][0])
            if mode == 'identity': doc['requests'][0]['url'] = src.ENDPOINT
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                src.replay(doc, values.__getitem__)

    def test_transport_exceptions_and_empty_failed_response_have_explicit_gaps(self):
        cap, m, _, _ = fixture()
        cap.opener = lambda *a, **kw: (_ for _ in ()).throw(TimeoutError())
        self.assertIsNone(cap.country('example'))
        evidence = cap.finish()
        self.assertEqual(evidence['complete_responses_retained'], 0)
        self.assertEqual(src.replay(json.loads(m.rows[evidence['manifest']['key']]), m.rows.__getitem__), {})
        cap, m, _, _ = fixture(page=b'', status=503)
        self.assertIsNone(cap.country('example'))
        evidence = cap.finish()
        self.assertEqual(evidence['complete_responses_retained'], 1)
        self.assertEqual(src.replay(json.loads(m.rows[evidence['manifest']['key']]), m.rows.__getitem__), {})


if __name__ == '__main__':
    unittest.main()
