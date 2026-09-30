"""Complete invented inputs and faithful S3 conditions; no actual services."""
from pathlib import Path
import copy
import hashlib
import io
import json
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'tests'), str(ROOT/'aws/shared'), str(Path(__file__).resolve().parents[1]/'source')]
from private_portfolio_test_support import Store, StoreError, load
import portfolio_publication as pub
import portfolio_risk_model as model

BUCKET, RISK, SNAPSHOT = 'synthetic-private-bucket', 'portfolio/risk.json', 'portfolio/snapshot.json'


class Mirror:
    def __init__(self):
        self.counter = 0
        self.tokens, self.calls = {}, []
        self.raw, self.rank, self.fail, self.bad_ack = None, 0, False, None
    def request(self, method, raw, **headers):
        self.calls.append((method, raw, headers))
        value = json.loads(raw)
        if method == 'POST':
            self.counter = max(self.counter, value['minimum_revision']) + 1
            token = f'{self.counter:08x}-0000-4000-8000-000000000000'
            self.tokens[self.counter] = token
            return {'ok': True, 'protocol': pub.PROTOCOL, 'revision': self.counter, 'token': token}
        if self.fail:
            raise pub.PublicationUnavailable('Synthetic mirror unavailable')
        rank = value['publication']['revision']
        assert headers['token'] == self.tokens[rank]
        assert headers['sha256'] == hashlib.sha256(raw).hexdigest()
        if rank < self.rank:
            raise pub.SupersededMirror('Synthetic newer mirror')
        if rank == self.rank and self.raw != raw:
            raise pub.PublicationUnavailable('Synthetic equal-rank conflict')
        self.raw, self.rank = raw, rank
        ack = {'ok': True, 'protocol': pub.PROTOCOL, 'revision': rank, 'body_sha256': headers['sha256'], 'status': 'published'}
        if self.bad_ack:
            self.bad_ack(ack)
        return ack


class MissingStore(Store):
    def get_object(self, **request):
        if request['Key'] not in self.docs:
            raise StoreError('NoSuchKey')
        return super().get_object(**request)


class PublicationOrdering(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((ROOT/'tests/fixtures/pre-portfolio-publication/complete-synthetic.json').read_bytes())
        self.store, self.mirror = MissingStore(), Mirror()
        self.priors = []
    def attempt(self, which='new', store=None):
        store = store or self.store
        store.docs[SNAPSHOT] = copy.deepcopy(self.fixture[which+'_bundle']['inputs']['snapshot'])
        result = pub.reserve_publication(store, BUCKET, RISK, self.mirror.request)
        result.update(source_etag=store.etag(SNAPSHOT), source_value_sha256=model.snapshot_value_identity(store.docs[SNAPSHOT])['value_sha256'])
        return result
    def publish(self, attempt, which='new', store=None, retain=None, payload=None):
        return pub.publish_ordered(copy.deepcopy(payload or self.fixture[which+'_payload']), attempt, store or self.store,
            BUCKET, RISK, SNAPSHOT, self.mirror.request, retain or self.priors.append)
    def test_whole_frozen_newer_then_older_no_longer_rolls_back_either_store(self):
        legacy = self.store.raw_body(RISK)
        old = self.attempt('old'); new = self.attempt('new')
        self.assertTrue(self.publish(new)['published'])
        self.assertEqual(self.publish(old, 'old')['status'], 'superseded')
        current = self.store.raw_body(RISK)
        self.assertEqual(current, self.mirror.raw); self.assertEqual(self.priors, [legacy])
        result = json.loads(current); result.pop('publication')
        self.assertEqual(result, self.fixture['new_payload'])
        self.assertNotIn(new['token'].encode(), current)
    def test_complete_raw_predecessor_is_retained_without_reserializing(self):
        raw = ' {"legacy":1.0,"unknown":"雪","null":null,"bool":false} \n'.encode()
        self.store.put_object(Key=RISK, Body=raw)
        self.assertTrue(self.publish(self.attempt())['published']); self.assertEqual(self.priors, [raw])
    def test_missing_current_uses_create_only_and_existing_current_uses_etag(self):
        for missing in [True, False]:
            self.setUp()
            if missing: del self.store.docs[RISK]
            self.publish(self.attempt())
            write = next(w for w in self.store.writes if w['Key'] == RISK)
            self.assertEqual(set(write) & {'IfMatch', 'IfNoneMatch'}, {'IfNoneMatch'} if missing else {'IfMatch'})
            self.assertEqual(write['CacheControl'], 'private, no-store')
    def test_same_attempt_retry_is_idempotent_and_conflicting_bytes_are_refused(self):
        ticket = self.attempt(); self.publish(ticket); count = len(self.store.writes)
        self.assertTrue(self.publish(ticket)['published']); self.assertEqual(len(self.store.writes), count)
        changed = copy.deepcopy(self.fixture['new_payload']); changed['unknown'] = False
        with self.assertRaises(pub.PublicationUnavailable): self.publish(ticket, payload=changed)
    def test_reservation_counter_does_not_reset_after_legacy_s3_overwrite(self):
        first = self.attempt(); self.publish(first)
        self.store.put_object(Key=RISK, Body=b'{"legacy":"late old Lambda"}')
        next_ticket = self.attempt()
        self.assertGreater(next_ticket['revision'], first['revision']); self.publish(next_ticket)
        self.assertEqual(self.mirror.rank, next_ticket['revision'])
    def test_failed_mirror_never_reports_success_and_same_bytes_can_retry(self):
        ticket = self.attempt(); self.mirror.fail = True
        with self.assertRaises(pub.PublicationUnavailable): self.publish(ticket)
        raw = self.store.raw_body(RISK); self.assertIsNone(self.mirror.raw)
        self.mirror.fail = False
        self.assertTrue(self.publish(ticket)['published']); self.assertEqual(self.mirror.raw, raw)
        self.assertEqual(len([w for w in self.store.writes if w['Key'] == RISK]), 1)
    def test_mismatched_ack_cannot_report_success(self):
        for key, value in [('ok', 1), ('protocol', 'unknown'), ('revision', True), ('body_sha256', '0'*64), ('status', 'queued')]:
            self.setUp(); ticket = self.attempt(); self.mirror.bad_ack = lambda ack, k=key, v=value: ack.update({k: v})
            with self.assertRaises(pub.PublicationUnavailable): self.publish(ticket)
    def test_changed_or_missing_source_before_write_leaves_current_untouched(self):
        for missing in [True, False]:
            self.setUp(); ticket = self.attempt(); before = self.store.raw_body(RISK)
            if missing: del self.store.docs[SNAPSHOT]
            else: self.store.docs[SNAPSHOT]['unknown'] = False
            self.assertEqual(self.publish(ticket)['status'], 'source_changed')
            self.assertEqual(self.store.raw_body(RISK), before); self.assertEqual(self.priors, []); self.assertIsNone(self.mirror.raw)
    def test_source_change_after_s3_write_is_explicit_partial_publication(self):
        ticket = self.attempt(); original = self.store.put_object
        def write(**request):
            value = original(**request)
            if request['Key'] == RISK: self.store.docs[SNAPSHOT]['changed_after_write'] = True
            return value
        self.store.put_object = write
        result = self.publish(ticket)
        self.assertEqual(result['status'], 'source_changed_after_s3'); self.assertFalse(result['published']); self.assertIsNone(self.mirror.raw)
        self.assertIn('publication', self.store.docs[RISK])
    def test_conditional_conflict_rechecks_newer_revision_instead_of_overwriting(self):
        ticket = self.attempt(); original = self.store.put_object
        newer = copy.deepcopy(self.fixture['new_payload']); newer['publication'] = {'schema_version': pub.PROTOCOL, 'revision': 100}
        def race(**request):
            if request['Key'] == RISK:
                original(Key=RISK, Body=model.canonical(newer)); raise StoreError('PreconditionFailed')
            return original(**request)
        self.store.put_object = race
        self.assertEqual(self.publish(ticket)['status'], 'superseded')
        self.assertEqual(self.store.docs[RISK], newer); self.assertIsNone(self.mirror.raw)
    def test_409_retries_with_conditions_and_contention_is_bounded(self):
        for failures in [1, 10]:
            self.setUp(); ticket = self.attempt(); original = self.store.put_object; calls = []
            def race(**request):
                if request['Key'] == RISK:
                    calls.append(request); self.assertIn('IfMatch', request)
                    if len(calls) <= failures: raise StoreError('ConditionalRequestConflict')
                return original(**request)
            self.store.put_object = race
            if failures == 1: self.assertTrue(self.publish(ticket)['published'])
            else:
                with self.assertRaisesRegex(pub.PublicationUnavailable, 'retry bound'): self.publish(ticket)
                self.assertEqual(len(calls), 4)
    def test_archive_failure_prevents_current_and_mirror_writes(self):
        ticket = self.attempt(); before = self.store.raw_body(RISK)
        def reject(raw): raise RuntimeError('Synthetic archive unavailable')
        with self.assertRaises(RuntimeError): self.publish(ticket, retain=reject)
        self.assertEqual(self.store.raw_body(RISK), before); self.assertIsNone(self.mirror.raw)
    def test_native_archive_callback_keeps_every_prior_byte_and_checks_collisions(self):
        _, _, env = load('portfolio-risk', self.store)
        raw = b' {"unknown":false,"number":1.0,"zero":0,"null":null}\n'
        env['retain_risk_publication'](raw); env['retain_risk_publication'](raw)
        key = model.ARCHIVE_PREFIX + 'publication-v1-' + hashlib.sha256(raw).hexdigest() + '.json'
        self.assertEqual(self.store.raw_body(key), raw)
        self.assertEqual(len([w for w in self.store.writes if w['Key'] == key]), 1)
        self.store.docs[key]['unknown'] = True
        with self.assertRaises(ValueError): env['retain_risk_publication'](raw)
    def test_bad_binding_or_unissued_revision_never_reaches_current_write(self):
        good = self.attempt()
        for field, value in [('source_value_sha256', 'b'*64), ('revision', True), ('token', 'not-a-token'), ('source_etag', 'unquoted'),
                             ('started_at', '2026-02-30T00:00:00Z'), ('started_at', None)]:
            ticket = {**good, field: value}
            with self.assertRaises(pub.PublicationUnavailable): self.publish(ticket)
        self.assertEqual(self.store.writes, []); self.assertIsNone(self.mirror.raw)
    def test_reservation_replies_validate_types_protocol_and_minimum(self):
        valid = self.mirror.request('POST', b'{"minimum_revision":0}')
        for field, value in [('ok', 1), ('protocol', 'bad'), ('revision', True), ('revision', 0), ('token', 'bad')]:
            with self.assertRaises(pub.PublicationUnavailable):
                pub.reserve_publication(self.store, BUCKET, RISK, lambda *a: {**valid, field: value})
        self.assertEqual(self.store.writes, [])
    def test_malformed_current_refuses_replacement_and_closes_original(self):
        for raw in [b'{"a":1,"a":2}', b'{"overflow":1e999}', b'{"unicode":"\\ud800"}', b'[]', b'\xff']:
            body = io.BytesIO(raw)
            self.store.get_object = lambda **kw: {'Body': body, 'ETag': '"synthetic"', 'ContentLength': len(raw)}
            with self.assertRaises(pub.PublicationUnavailable): pub.read_current(self.store, BUCKET, RISK)
            self.assertTrue(body.closed)
    def test_missing_identity_bad_length_encoding_deadline_and_truncation_close(self):
        for metadata in [{'ETag': None}, {'ContentLength': True}, {'ContentLength': 4}, {'ContentEncoding': 'gzip'}]:
            body = io.BytesIO(b'{}')
            self.store.get_object = lambda **kw: {'Body': body, 'ETag': '"synthetic"', **metadata}
            with self.assertRaises(pub.PublicationUnavailable): pub.read_current(self.store, BUCKET, RISK)
            self.assertTrue(body.closed)
        body = io.BytesIO(b'{}')
        with patch.object(pub.time, 'monotonic', return_value=20):
            with self.assertRaises(pub.PublicationUnavailable): pub.read_bytes(body, 10, 20)
        self.assertTrue(body.closed)
    def test_whole_predecessors_remain_inert_and_unchanged_inputs_recalculate(self):
        raw = (ROOT/'tests/fixtures/pre-risk-calculation/stage448-predecessor-portfolio_risk_model.py.txt').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), '9fe0b098fa1ffa9a666e8e843f1c9ea52724ac5f065c638101afef217c7dc6a4')
        for name in ['old', 'new']:
            bundle = self.fixture[name+'_bundle']
            with self.assertRaisesRegex(ValueError,'code/schema mismatch'): model.replay(bundle)
            current,output = model.freeze(**bundle['inputs'])
            self.assertEqual(model.replay(current),output)
            expected = copy.deepcopy(self.fixture[name+'_payload'])
            expected.pop('replay'); expected.pop('alerts_sent')
            expected['schema_version'] = '2.0.1'
            self.assertEqual(output,expected)



if __name__ == '__main__': unittest.main()
