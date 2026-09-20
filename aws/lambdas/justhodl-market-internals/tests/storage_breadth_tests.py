"""Fault-injection tests using synthetic sources and a conditional in-memory S3."""
from pathlib import Path
from datetime import datetime
import copy, io, json, sys, time, unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT/'aws/shared'))
from native_breadth_tests import fixtures, DAYS, AT
import breadth_research_model as m
import breadth_research_store as s
from breadth_series import native_suffix


class S3Error(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class MemoryS3:
    def __init__(self): self.data = {}; self.writes = []; self.corrupt = None; self.force_conflict = False
    def get_object(self, Bucket, Key):
        if Key not in self.data: raise S3Error('NoSuchKey')
        raw = self.data[Key]
        return {'Body': io.BytesIO(raw), 'ETag': m.sha(raw)}
    def put_object(self, Bucket, Key, Body, **kw):
        if (kw.get('IfNoneMatch') == '*' and Key in self.data or
            kw.get('IfMatch') and (Key not in self.data or kw['IfMatch'] != m.sha(self.data[Key])) or
            self.force_conflict and Key == s.CURRENT): raise S3Error('PreconditionFailed')
        self.data[Key] = Body if Key != self.corrupt else b'corrupt'
        self.writes.append((Key, kw))


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.db = MemoryS3(); self.fixtures = fixtures(); self.read = s.reader(self.db, 'test')
        self.sources = s.collect(self.db, 'test', DAYS, 'test-token', time.monotonic()+30,
            fetch=lambda day, token, deadline: {k: v for k, v in self.fixtures[day].items() if k != 'evidence'})
        self.inputs = {'contract': 'breadth-native-inputs.v1', 'expected_days': DAYS, 'sources': self.sources,
            'started_at': '2026-09-20T11:00:00+00:00', 'generated_at': '2026-09-20T11:10:00+00:00'}
        self.output = s.compile_output(self.inputs, self.read)

    def test_exact_original_retention_and_replay(self):
        expected = (Path(__file__).parent/'fixtures/expected-output.sha256').read_text().strip()
        self.assertEqual(m.sha(m.encoded(self.output)), expected)
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        self.assertEqual(s.replay(ref, self.read), self.output)
        self.assertEqual(len([k for k in self.db.data if k.startswith(s.PRIVATE)]), 253)
        for key, raw in self.db.data.items():
            if key.startswith(s.PREFIX):
                self.assertNotIn(b'"results":', raw)
                self.assertNotIn(b'Bearer test-token', raw)

    def test_tampered_original_aborts_replay(self):
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        key = self.sources[DAYS[-1]]['evidence']['key']; self.db.data[key] += b' '
        with self.assertRaisesRegex(ValueError, 'original bytes differ'): s.replay(ref, self.read)

    def test_compiler_substitution_aborts(self):
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        manifest = json.loads(self.db.data[ref['manifest_key']]); key = manifest['compilers']['breadth_research_model']['key']
        self.db.data[key] += b'# changed'
        with self.assertRaisesRegex(ValueError, 'compiler'): s.replay(ref, self.read)

    def test_conditional_publication_retains_entire_previous_packet(self):
        old = m.encoded({'generated_at': '2026-09-19T22:40:00+00:00', 'legacy': {'whole': [1, 2, 3]}})
        self.db.data[s.CURRENT] = old
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        packet = {**self.output, 'replay': ref}
        self.assertTrue(s.publish(self.db, 'test', packet))
        self.assertEqual(self.db.data[s.PRIVATE+m.sha(old)+'.bin'], old)
        self.assertEqual(json.loads(self.db.data[s.CURRENT]), packet)
        self.assertEqual(self.db.writes[-1][1]['CacheControl'], 'no-store')

    def test_older_or_conflicting_clock_cannot_replace_current(self):
        packet = copy.deepcopy(self.output); self.db.data[s.CURRENT] = m.encoded(packet)
        older = {**packet, 'generated_at': '2026-09-20T10:00:00+00:00'}
        self.assertFalse(s.publish(self.db, 'test', older))
        changed = {**packet, 'state': 'changed'}
        with self.assertRaisesRegex(ValueError, 'same-clock'): s.publish(self.db, 'test', changed)

    def test_immutable_collision_and_publication_conflict_abort(self):
        raw = b'{}'; key = s.PREFIX+'outputs/'+m.sha(raw)+'.json'; self.db.data[key] = b'different'
        with self.assertRaisesRegex(ValueError, 'readback'): s.immutable(self.db, 'test', key, raw)
        self.db.force_conflict = True
        with self.assertRaises(RuntimeError): s.publish(self.db, 'test', self.output)
        self.assertNotIn(s.CURRENT, self.db.data)

    def test_original_clock_and_private_path_are_scoped(self):
        self.sources[DAYS[-1]]['acquired_at'] = '2026-09-20T10:59:59+00:00'
        with self.assertRaisesRegex(ValueError, 'outside this collection'): s.compile_output(self.inputs, self.read)
        for key in ('brain.json', 'portfolio.json', 'audit-private/another-domain/secret.bin', '../'+s.CURRENT):
            with self.assertRaises(ValueError): self.read(key)

    def test_failed_acquisition_remains_missing_and_replayable(self):
        self.sources[DAYS[-1]] = {'error': 'provider_http_429'}
        output = s.compile_output(self.inputs, self.read)
        self.assertEqual(output['quality']['status'], 'unavailable')
        ref = s.retain(self.db, 'test', self.inputs, output)
        self.assertEqual(s.replay(ref, self.read), output)
        self.assertTrue(all(row[1] is None for row in output['latest'].values()))

    def test_duplicate_request_returns_status_without_a_new_collection(self):
        key = s.request_key('synthetic-acceptance-1')
        self.db.data[key] = m.encoded({'status': 'running', 'execution_id': 'first'})
        with patch.object(s, 'collect', side_effect=AssertionError('must not recollect')):
            result = s.run(self.db, 'test', 'synthetic-acceptance-1', 'second', lambda: 'unused')
        self.assertEqual(result['execution_id'], 'first')

    def test_completed_request_replays_and_publishes_only_the_public_producer(self):
        clocks = ['2026-09-20T11:00:00+00:00', '2026-09-20T11:10:00+00:00', '2026-09-20T11:11:00+00:00']
        with patch.object(s, 'now', side_effect=clocks), patch.object(s, 'collect', return_value=self.sources):
            result = s.run(self.db, 'test', 'synthetic-complete-1', 'aws-execution-1', lambda: 'test-token')
        self.assertEqual(result['status'], 'complete'); self.assertTrue(result['published'])
        packet = json.loads(self.db.data[s.CURRENT]); self.assertEqual(s.replay(packet['replay'], self.read), self.output)
        status = json.loads(self.db.data[s.request_key('synthetic-complete-1')])
        self.assertEqual(status, result)
        with patch.object(s, 'collect', side_effect=AssertionError('no repeated collection')):
            self.assertEqual(s.run(self.db, 'test', 'synthetic-complete-1', 'later', lambda: ''), result)

    def test_corrupted_retention_fails_without_a_current_publication(self):
        self.sources[DAYS[-1]]['evidence']['sha256'] = '0'*64
        clocks = ['2026-09-20T11:00:00+00:00', '2026-09-20T11:10:00+00:00', '2026-09-20T11:11:00+00:00']
        with patch.object(s, 'now', side_effect=clocks), patch.object(s, 'collect', return_value=self.sources):
            with self.assertRaises(RuntimeError): s.run(self.db, 'test', 'synthetic-failed-1', 'aws-execution-1', lambda: 'test-token')
        self.assertNotIn(s.CURRENT, self.db.data)
        status = json.loads(self.db.data[s.request_key('synthetic-failed-1')])
        self.assertEqual((status['status'], status['phase']), ('failed', 'compile'))

    def test_constituent_flags_reconcile_exact_denominators(self):
        rows = self.output['current_constituents']; coverage = self.output['coverage'][DAYS[-1]]
        self.assertEqual(len(rows), coverage['current_filter_population'])
        for field, num, den in (('above_50', 'sma50_numerator', 'sma50_denominator'), ('above_200', 'sma200_numerator', 'sma200_denominator')):
            self.assertEqual(sum(r[field] is True for r in rows), coverage[num])
            self.assertEqual(sum(r[field] is not None for r in rows), coverage[den])

    def test_numeric_consumer_preserves_gaps_and_expires(self):
        def bound(doc): return {**doc, 'replay': {'output_sha256': m.sha(m.encoded(doc))}}
        at = datetime.fromisoformat(AT)
        packet = bound(self.output)
        suffix = native_suffix(packet, 'PCT_ABOVE_200DMA', '2025-01-01', at)
        self.assertEqual(len(suffix), 54)  # inclusive 200-session warm-up
        self.assertEqual(suffix[DAYS[-1]], 66.666667)
        changed = copy.deepcopy(self.output); changed['series']['PCT_ABOVE_200DMA'][DAYS[-3]] = None
        self.assertEqual(len(native_suffix(bound(changed), 'PCT_ABOVE_200DMA', '2025-01-01', at)), 2)
        changed['series']['PCT_ABOVE_200DMA'][DAYS[-1]] = None
        self.assertEqual(native_suffix(bound(changed), 'PCT_ABOVE_200DMA', '2025-01-01', at), {})
        self.assertEqual(native_suffix(packet, 'ADVANCERS', '2025-01-01', datetime.fromisoformat('2026-09-25T12:00:00+00:00')), {})
        packet['as_of'] = DAYS[-2]
        self.assertEqual(native_suffix(packet, 'ADVANCERS', '2025-01-01', at), {})


class SourceTests(unittest.TestCase):
    def test_request_does_not_put_credential_in_url_and_reflection_rejected(self):
        class Response(io.BytesIO): status = 200
        class Opener:
            def open(self, request, timeout):
                self.request = request; return Response(b'{"token":"private-value"}')
        opener = Opener()
        result = s.acquire(DAYS[-1], 'private-value', time.monotonic()+10, opener)
        self.assertEqual(result, {'error': 'unsafe_provider_reflection'})
        self.assertNotIn('private-value', opener.request.full_url)
        self.assertEqual(opener.request.get_header('Authorization'), 'Bearer private-value')

    def test_missing_key_or_expired_budget_never_fetches(self):
        with patch('urllib.request.build_opener', side_effect=AssertionError('no provider call')):
            self.assertEqual(s.acquire(DAYS[-1], '', time.monotonic()+10)['error'], 'managed_source_credential_unavailable')
            self.assertEqual(s.acquire(DAYS[-1], 'secret', time.monotonic()-1)['error'], 'collection_budget_exhausted')

    def test_redirect_and_request_identity_rejected(self):
        with self.assertRaises(ValueError): s.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://elsewhere.invalid')
        with self.assertRaises(ValueError): s.request_key('../escape')


if __name__ == '__main__': unittest.main()
