"""Fault-injection tests using synthetic sources and a conditional in-memory S3."""
from pathlib import Path
from datetime import datetime
import copy, io, json, sys, time, unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT/'aws/shared'))
from native_aaii_tests import fixtures, AT
DAYS = ("main", "results")
import aaii_research_model as m
import aaii_research_store as s


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
        self.sources = s.collect(self.db, 'test', time.monotonic()+30,
            fetch=lambda day, deadline: {k: v for k, v in self.fixtures[day].items() if k != 'evidence'})
        self.inputs = {'contract': 'aaii-native-inputs.v1', 'sources': self.sources,
            'started_at': '2026-09-20T13:59:30+00:00', 'generated_at': '2026-09-20T14:00:00+00:00'}
        self.output = s.compile_output(self.inputs, self.read)

    def test_exact_original_retention_and_replay(self):
        expected = (Path(__file__).parent/'fixtures/expected-output.sha256').read_text().strip()
        self.assertEqual(m.sha(m.encoded(self.output)), expected)
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        self.assertEqual(s.replay(ref, self.read), self.output)
        self.assertEqual(len([k for k in self.db.data if k.startswith(s.PRIVATE)]), 2)
        for key, raw in self.db.data.items():
            if key.startswith(s.PREFIX):
                self.assertNotIn(b'<!DOCTYPE html>', raw) if not key.endswith('.py') else None
                self.assertNotIn(b'Bearer test-token', raw)

    def test_tampered_original_aborts_replay(self):
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        key = self.sources[DAYS[-1]]['evidence']['key']; self.db.data[key] += b' '
        with self.assertRaisesRegex(ValueError, 'original bytes differ'): s.replay(ref, self.read)

    def test_compiler_substitution_aborts(self):
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        manifest = json.loads(self.db.data[ref['manifest_key']]); key = manifest['compilers']['aaii_research_model']['key']
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
        self.assertTrue(all(v is None for v in output['latest'].values()))

    def test_duplicate_request_returns_status_without_a_new_collection(self):
        key = s.request_key('synthetic-acceptance-1')
        self.db.data[key] = m.encoded({'status': 'running', 'execution_id': 'first'})
        with patch.object(s, 'collect', side_effect=AssertionError('must not recollect')):
            result = s.run(self.db, 'test', 'synthetic-acceptance-1', 'second', 180)
        self.assertEqual(result['execution_id'], 'first')

    def test_completed_request_replays_and_publishes_only_the_public_producer(self):
        clocks = ['2026-09-20T13:59:30+00:00', '2026-09-20T14:00:00+00:00', '2026-09-20T14:00:05+00:00']
        with patch.object(s, 'now', side_effect=clocks), patch.object(s, 'collect', return_value=self.sources):
            result = s.run(self.db, 'test', 'synthetic-complete-1', 'aws-execution-1', 180)
        self.assertEqual(result['status'], 'complete'); self.assertTrue(result['published'])
        packet = json.loads(self.db.data[s.CURRENT]); self.assertEqual(s.replay(packet['replay'], self.read), self.output)
        status = json.loads(self.db.data[s.request_key('synthetic-complete-1')])
        self.assertEqual(status, result)
        with patch.object(s, 'collect', side_effect=AssertionError('no repeated collection')):
            self.assertEqual(s.run(self.db, 'test', 'synthetic-complete-1', 'later', 180), result)

    def test_corrupted_retention_fails_without_a_current_publication(self):
        self.sources[DAYS[-1]]['evidence']['sha256'] = '0'*64
        clocks = ['2026-09-20T13:59:30+00:00', '2026-09-20T14:00:00+00:00', '2026-09-20T14:00:05+00:00']
        with patch.object(s, 'now', side_effect=clocks), patch.object(s, 'collect', return_value=self.sources):
            with self.assertRaises(RuntimeError): s.run(self.db, 'test', 'synthetic-failed-1', 'aws-execution-1', 180)
        self.assertNotIn(s.CURRENT, self.db.data)
        status = json.loads(self.db.data[s.request_key('synthetic-failed-1')])
        self.assertEqual((status['status'], status['phase']), ('failed', 'compile'))

    def test_replay_cli_reports_survey_weeks(self):
        sys.path.insert(0, str(ROOT/'scripts'))
        from replay_aaii_research import verify
        ref = s.retain(self.db, 'test', self.inputs, self.output)
        proof = verify({**self.output, 'replay': ref}, self.read)
        self.assertEqual(proof['survey_weeks'], 22)
        self.assertFalse(proof['forecast_qualified'])

    def test_source_identity_and_collection_clock_are_enforced(self):
        for mutate in (lambda d:d['sources']['main']['evidence'].update(source_url='https://example.com'),
                       lambda d:d.update(generated_at='2026-09-20T14:02:00+00:00')):
            inputs = copy.deepcopy(self.inputs); mutate(inputs)
            with self.assertRaises(ValueError): s.compile_output(inputs, self.read)

    def test_degraded_new_attempt_preserves_entire_last_good(self):
        self.db.data[s.CURRENT] = m.encoded(self.output)
        inputs = copy.deepcopy(self.inputs); inputs['generated_at'] = '2026-09-20T14:00:01+00:00'
        inputs['sources']['results'] = {'error': 'provider_http_403'}
        degraded = s.compile_output(inputs, self.read)
        self.assertTrue(s.publish(self.db, 'test', degraded))
        self.assertEqual(degraded['quality']['status'], 'unavailable')
        self.assertEqual(self.db.data[s.PRIVATE+m.sha(m.encoded(self.output))+'.bin'], m.encoded(self.output))

    def test_provider_errors_are_bounded_without_error_body_publication(self):
        import urllib.error
        class Opener:
            def open(self, request, timeout):
                raise urllib.error.HTTPError(request.full_url, 403, 'untrusted body', {}, io.BytesIO(b'private injected data'))
        result = s.acquire('main', time.monotonic()+10, Opener())
        self.assertEqual(result, {'error': 'provider_http_403'})
        with self.assertRaises(ValueError): s.source_url('https://unapproved.example')

    def test_byte_bound_closes_original_stream(self):
        stream = io.BytesIO(b'x'*(s.MAX+1))
        with self.assertRaises(ValueError): s.bounded(stream)
        self.assertTrue(stream.closed)


if __name__ == '__main__': unittest.main()
