"""Whole native rehearsal, durable acquisition, permission and publication tests."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
import io, json, sys, threading, unittest, urllib.error

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT/'aws/lambdas/justhodl-signal-board/source'
sys.path.insert(0, str(SOURCE))
import board_store as store
import lambda_function as handler


class Conflict(Exception):
    response = {'Error': {'Code': 'PreconditionFailed'}}


class Storage:
    def __init__(self):
        self.objects = {store.CURRENT: b'{"generated_at":"2026-09-26T06:15:00Z","entire_legacy":true}'}
        self.puts = []; self.reads = []; self.lock = threading.Lock(); self.conflicts = 0
    def put_object(self, **kw):
        with self.lock:
            key = kw['Key']; old = self.objects.get(key)
            if kw.get('IfNoneMatch') == '*' and old is not None: raise Conflict()
            if 'IfMatch' in kw and (old is None or kw['IfMatch'] != store.sha(old)): raise Conflict()
            if 'IfMatch' in kw and self.conflicts:
                self.conflicts -= 1; raise Conflict()
            self.objects[key] = kw['Body']; self.puts.append(kw)
    def get_object(self, **kw):
        with self.lock:
            self.reads.append(kw['Key']); raw = self.objects[kw['Key']]
            return {'Body': io.BytesIO(raw), 'ETag': store.sha(raw)}


class Tests(unittest.TestCase):
    def setUp(self): self.client = Storage(); self.requests = []
    def acquire(self, key, timeout):
        attempts = [json.loads(v) for k, v in self.client.objects.items() if '/requests/' in k and k.endswith('.json')]
        self.assertTrue(any(x.get('source_key') == key and x.get('status') == 'attempt_recorded' for x in attempts))
        self.requests.append(key)
        if key == 'data/ici-flows.json': return b'', 403, {}
        if key == 'screener/mean-reversion.json': raise TimeoutError('synthetic')
        return store.encoded({'generated_at': '2026-09-26T06:00:00Z', 'source_key': key,
                              'signal': 2, 'calls_eligible': True, 'history': [0, None, -1]*400}), 200, {}
    def run_native(self):
        with patch.object(store, 'now', return_value='2026-09-26T12:17:00+00:00'):
            result = store.run(self.client, 'test', '2026-09-26T12:00:00+00:00', self.acquire)
        return result, json.loads(self.client.objects[store.CURRENT])
    def test_entire_native_acquisition_publication_and_independent_replay(self):
        old = self.client.objects[store.CURRENT]; result, packet = self.run_native()
        self.assertTrue(result['published']); self.assertEqual(len(self.requests), 96); self.assertEqual(len(set(self.requests)), 96)
        proof = store.replay(packet, store.reader(self.client, 'test'))
        self.assertEqual(proof['whole_responses_checked'], 95); self.assertEqual(len(packet['engines']), 99)
        self.assertEqual(packet['source_status_counts']['private_input_excluded'], 2)
        self.assertEqual(packet['source_status_counts']['http_error'], 1)
        self.assertEqual(packet['source_status_counts']['transport_unavailable'], 1)
        self.assertIsNone(packet['composite_signal']); self.assertTrue(all(r['signal'] is None for r in packet['engines']))
        self.assertFalse(packet['original_provider_verified'])
        self.assertEqual(self.client.objects[store.PRIVATE+store.sha(old)+'.bin'], old)
        self.assertFalse(set(self.client.reads) & store.candidate.PRIVATE)
        self.assertTrue(all(x['CacheControl'] == 'no-store' for x in self.client.puts if x['Key'] == store.CURRENT))
    def test_duplicate_slot_never_requests_a_source_twice(self):
        self.run_native(); before = len(self.requests)
        with self.assertRaises(Conflict): self.run_native()
        self.assertEqual(len(self.requests), before)
    def test_compiler_qualification_and_whole_predecessor_are_pinned(self):
        store.qualified()
        for name in store.QUALIFIED:
            self.assertEqual((SOURCE/name).read_bytes(), (ROOT/'aws/ops/checks'/name).read_bytes())
        old = (SOURCE.parent/'tests/legacy-lambda_function.py.txt').read_bytes()
        self.assertEqual(len(old), 69413)
        self.assertEqual(store.sha(old), '1c702805519612465c5413313eadee99a0a869bdcdb8a4baf025b4d5d539dbbe')
        self.assertEqual(store.qualified(), json.loads((ROOT/'assets/signal-board-registry.json').read_bytes())['feeds'])
    def test_tampered_whole_source_rejects_publication(self):
        _, packet = self.run_native(); before = self.client.objects[store.CURRENT]
        ref = next(x['original'] for x in packet['sources'].values() if x['original'])
        self.client.objects[ref['key']] += b'corrupt'
        with self.assertRaises(ValueError): store.publish(self.client, 'test', packet)
        self.assertEqual(self.client.objects[store.CURRENT], before)
    def test_forged_authority_or_missing_rows_reject_even_with_new_view_hash(self):
        _, packet = self.run_native()
        for mutate in (lambda p: p.update(calls_eligible=True), lambda p: p['engines'].pop()):
            p = deepcopy(packet); mutate(p)
            with self.assertRaises(ValueError): store.publish(self.client, 'test', p)
    def test_older_or_conflicting_head_cannot_replace_newer_bytes(self):
        _, packet = self.run_native()
        newer = store.encoded({'generated_at': '2026-09-26T18:00:00Z'})
        self.client.objects[store.CURRENT] = newer
        self.assertFalse(store.publish(self.client, 'test', packet)); self.assertEqual(self.client.objects[store.CURRENT], newer)
        self.client.objects[store.CURRENT] = store.encoded({'generated_at': packet['generated_at']})
        with self.assertRaises(ValueError): store.publish(self.client, 'test', packet)
    def test_conditional_conflicts_are_bounded_without_reacquiring(self):
        self.client.conflicts = 4
        with self.assertRaises(RuntimeError): self.run_native()
        self.assertEqual(len(self.requests), 96)
        self.assertNotIn('contract', json.loads(self.client.objects[store.CURRENT]))
    def test_whole_empty_http_errors_and_redirects_are_retained(self):
        seen = []
        def transport(req, timeout):
            seen.append(req)
            raise urllib.error.HTTPError(req.full_url, 302, 'redirect', {'Location': 'https://private.invalid', 'Content-Length': '0', 'Set-Cookie': 'secret'}, io.BytesIO(b''))
        raw, status, headers = store.acquire('data/ici-flows.json', transport=transport)
        self.assertEqual((raw, status), (b'', 302)); self.assertNotIn('set-cookie', headers)
        self.assertEqual(len(seen), 1); self.assertTrue(seen[0].full_url.endswith('?exact=1&nogen=1'))
        self.assertIsNone(store.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://private.invalid'))
    def test_private_and_unknown_source_or_truncated_body_never_accepted(self):
        for key in (*store.candidate.PRIVATE, 'data/missing.json', '../secret'):
            with self.assertRaises(ValueError): store.acquire(key, transport=lambda *a, **k: self.fail('must not request'))
        response = io.BytesIO(b'a'); response.status = 200; response.headers = {'Content-Length': '2'}
        with self.assertRaises(ValueError): store.acquire('data/ici-flows.json', transport=lambda *a, **k: response)
        with self.assertRaises(ValueError): store.bounded(io.BytesIO(b'abc'), 2)
    def test_http_and_validation_never_create_aws_client(self):
        with patch.object(handler.boto3, 'client', side_effect=AssertionError('No AWS on public read')):
            self.assertEqual(handler.lambda_handler({'httpMethod': 'GET'})['statusCode'], 307)
            self.assertEqual(handler.lambda_handler({'validate_only': True})['statusCode'], 200)
            with self.assertRaises(ValueError): handler.lambda_handler({})
    def test_six_hour_slot_is_stable_across_retries_and_requires_valid_time(self):
        self.assertEqual(store.slot({'time': '2026-09-26T12:15:00Z'}), store.slot({'time': '2026-09-26T12:15:00Z', 'id': 'retry'}))
        self.assertEqual(store.slot({}, datetime(2026, 9, 26, 13, tzinfo=timezone.utc)), '2026-09-26T12:00:00+00:00')
        with self.assertRaises(ValueError): store.slot({'time': 'bad'})
    def test_schedule_is_preserved_and_source_has_no_paid_ai_or_alerts(self):
        sys.path.insert(0, str(ROOT/'scripts'))
        import normalize_lambda_config as normalize
        cfg = json.loads((SOURCE.parent/'config.json').read_bytes())
        self.assertEqual(cfg['schedule'], 'cron(15 0/6 * * ? *)')
        self.assertEqual(cfg['preserved_schedule_reference']['rule_name'], 'signal-board-3h')
        normalized = normalize.normalize_config(cfg)
        self.assertNotIn('schedule', normalized)
        self.assertEqual(normalized['release_schedule_note']['binding_action'], 'PRESERVE_EXISTING')
        text = '\n'.join(p.read_text(encoding='utf-8') for p in SOURCE.glob('*.py'))
        for bad in ('.invoke(', 'anthropic', 'llm_router', 'put_events(', 'send_message('): self.assertNotIn(bad, text)


if __name__ == '__main__': unittest.main(verbosity=2)
