"""Adversarial integration tests; AWS, providers and messaging are never called."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import gzip
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws' / 'shared'))
import research_network as N
import research_network_store as S
import research_network_consumer as C
import research_network_prospective as P
from research_network_registry import SOURCES, SUBSCRIPTIONS, GROUPS
from system_events import publish_many
from event_outbox import advance, read_state

NOW = datetime(2026, 10, 7, 17, tzinfo=timezone.utc)


class Missing(Exception):
    response = {'Error': {'Code': 'NoSuchKey'}}


class Conflict(Exception):
    response = {'Error': {'Code': 'PreconditionFailed'}}


class Store:
    def __init__(self):
        self.data = {}; self.reads = []; self.writes = []; self.stored = NOW - timedelta(minutes=1)

    def get_object(self, *, Key, **kwargs):
        self.reads.append(Key)
        if Key not in self.data: raise Missing(Key)
        raw = self.data[Key]
        return {'Body': io.BytesIO(raw), 'ContentLength': len(raw), 'ETag': hashlib.sha256(raw).hexdigest(), 'LastModified': self.stored}

    def put_object(self, *, Key, Body, **kwargs):
        if kwargs.get('IfNoneMatch') and Key in self.data: raise Conflict(Key)
        if kwargs.get('IfMatch') and (Key not in self.data or hashlib.sha256(self.data[Key]).hexdigest() != kwargs['IfMatch']): raise Conflict(Key)
        self.data[Key] = Body; self.writes.append(Key)
        return {'ETag': hashlib.sha256(Body).hexdigest()}


def native(rows=None):
    return {'generated_at': NOW.isoformat(), 'calls_eligible': True, 'companies': rows or [{'ticker': 'NVDA', 'direction': 'UP', 'pe': 0, 'period_end': '2026-06-30'}]}


class NetworkTests(unittest.TestCase):
    def setUp(self):
        self.spec = SOURCES['fundamentals']

    def ingest(self, doc=None, sha='a'*64):
        return N.ingest(self.spec, doc or native(), sha, NOW)

    def test_every_integration_has_native_inputs_and_no_private_source(self):
        from private_artifact import public_source_allowed
        self.assertEqual(len(GROUPS), 16)
        self.assertTrue(all(public_source_allowed(v['key']) for v in SOURCES.values()))
        for _, _, names in GROUPS.values(): self.assertTrue(set(names.split()) <= set(SOURCES))

    def test_zero_preserved_and_authority_never_inherited(self):
        meta, records = self.ingest()
        self.assertEqual(records[0]['facts']['pe'], 0)
        self.assertTrue(meta['reported_permissions']['calls_eligible'])
        for obj in (meta, *records):
            for key, value in N.PERMISSIONS.items(): self.assertEqual(obj[key], value)

    def test_crypto_and_listed_security_do_not_merge(self):
        stock = N.identity('BTC', {}, self.spec)
        crypto = N.identity('BTC', {}, SOURCES['crypto-funding'])
        self.assertNotEqual(stock['id'], crypto['id'])
        self.assertFalse(stock['instrument_identity_qualified'])

    def test_no_symbol_aliases_or_stringified_objects(self):
        self.assertNotEqual(N.identity('GOOG', {}, self.spec)['id'], N.identity('GOOGL', {}, self.spec)['id'])
        self.assertNotEqual(N.identity('BRK.B', {}, self.spec)['id'], N.identity('BRK-B', {}, self.spec)['id'])
        for invalid in ({'symbol': 'NVDA'}, True, "{'SYMBOL': 'NVDA'}", '<img src=x>'):
            self.assertIsNone(N.identity(invalid, {}, self.spec))

    def test_mixed_asset_class_must_be_explicit(self):
        self.assertTrue(N.identity('BTC', {}, SOURCES['khalid'])['scope'].startswith('unresolved'))
        self.assertEqual(N.identity('BTC', {'asset_class': 'crypto'}, SOURCES['khalid'])['scope'], 'crypto')

    def test_clocks_never_promote_publication_to_observation(self):
        doc = native(); doc.pop('generated_at'); doc['as_of'] = '2026-10-07'
        meta, _ = self.ingest(doc)
        self.assertEqual(meta['publication_freshness'], 'unknown')
        self.assertFalse(meta['observation_freshness_verified'])
        for delta, expected in ((timedelta(days=1), 'future'), (-timedelta(days=3), 'stale')):
            doc['generated_at'] = (NOW+delta).isoformat()
            self.assertEqual(self.ingest(doc)[0]['publication_freshness'], expected)

    def test_schema_drift_does_not_publish_partial_rows(self):
        doc = native([{'ticker': 'NVDA'}, 'malformed'])
        meta, records = self.ingest(doc)
        self.assertEqual(meta['status'], 'adapter_mismatch'); self.assertEqual(records, [])

    def test_empty_is_not_unavailable_or_bearish(self):
        doc = native(); doc['companies'] = []
        meta, records = self.ingest(doc)
        self.assertTrue(meta['research_available']); self.assertEqual(meta['status'], 'empty'); self.assertEqual(records, [])

    def test_census_rejects_misaligned_columns(self):
        spec = SOURCES['fundamental-census-matrix']
        meta, records = N.ingest(spec, {'tickers': ['A', 'B'], 'cols': {'pe': [0]}}, 'a'*64, NOW)
        self.assertEqual(meta['status'], 'adapter_mismatch'); self.assertEqual(records, [])

    def test_disagreements_keep_horizons_and_do_not_become_votes(self):
        meta, records = self.ingest(native([{'ticker': 'NVDA', 'direction': 'UP', 'horizon_days': 5},
                                           {'ticker': 'NVDA', 'direction': 'DOWN', 'horizon_days': 90}]))
        dossier = N.dossier('listed_security:NVDA', records, {'fundamentals': meta})
        self.assertEqual(len(dossier['thesis']['contradictions']), 1)
        self.assertEqual(len(dossier['thesis']['horizons']), 2)
        self.assertEqual(dossier['independent_investment_votes'], 0)

    def test_error_rows_and_scores_do_not_become_forecasts(self):
        for row in ({'ticker': 'NVDA', 'score': 99}, {'ticker': 'NVDA', 'direction': 'UP', 'error': 'provider unavailable'}):
            _, records = self.ingest(native([row])); self.assertIsNone(N.explicit_direction(records[0]))

    def test_identical_occurrences_are_not_independent_evidence(self):
        meta, rows = self.ingest(); dossier = N.dossier('listed_security:NVDA', rows+rows, {'fundamentals': meta})
        self.assertEqual(len(dossier['evidence']), 1); self.assertEqual(dossier['independent_investment_votes'], 0)

    def test_new_publication_unchanged_observation_has_no_economic_change(self):
        meta, rows = self.ingest(); before = N.dossier('listed_security:NVDA', rows, {'fundamentals': meta})
        meta, rows = self.ingest(sha='b'*64); after = N.dossier('listed_security:NVDA', rows, {'fundamentals': meta}, before)
        self.assertEqual(after['thesis']['changes']['changed_observation_sources'], [])
        self.assertNotEqual(after['evidence_ids'], before['evidence_ids'])
        doc = native(); doc['companies'][0]['pe'] = 1
        meta, rows = self.ingest(doc, 'c'*64); changed = N.dossier('listed_security:NVDA', rows, {'fundamentals': meta}, before)
        self.assertEqual(changed['thesis']['changes']['changed_observation_sources'], ['fundamentals'])

    def test_json_rejects_ambiguity_and_nonfinite_values(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'{'):
            with self.assertRaises(ValueError): S.strict_json(raw)

    def test_complete_read_closes_body_and_rejects_truncation(self):
        stream = io.BytesIO(b'{}')
        client = types.SimpleNamespace(get_object=lambda **kw: {'Body': stream, 'ContentLength': 3})
        with self.assertRaises(ValueError): S.read(client, 'test', 'data/a.json')
        self.assertTrue(stream.closed)

    def test_private_path_is_rejected_before_get(self):
        store = Store()
        for key in ('portfolio/snapshot.json', 'data/brain.json', 'data/../portfolio/snapshot.json'):
            with self.assertRaises(ValueError): S.read(store, 'test', key)
        self.assertEqual(store.reads, [])

    def build(self):
        store = Store(); store.data[self.spec['key']] = N.canonical(native())
        manifest = S.publish_network(store, 'test', now=NOW)
        return store, manifest

    def test_end_to_end_native_read_archive_shard_and_every_consumer_receipt(self):
        store, manifest = self.build()
        entity = S.read_entity(store, 'test', manifest, 'listed_security:NVDA')
        self.assertEqual(entity['evidence'][0]['facts']['pe'], 0)
        for consumer in SUBSCRIPTIONS:
            result = C.consume(store, 'test', consumer, now=NOW,
                               **({'positions': []} if consumer == 'portfolio-risk' else {}))
            self.assertEqual(result['status'], 'available', consumer)
            self.assertEqual(result['publication_id'], manifest['publication_id'])
        self.assertEqual(store.writes[-1], S.KEY)  # pointer is last
        self.assertEqual(len(manifest['specialists']), 16)

    def test_failed_shard_keeps_previous_pointer(self):
        store, first = self.build(); old = store.data[S.KEY]; original = store.put_object
        def fail(**kwargs):
            if '/publications/' in kwargs['Key']: raise RuntimeError('injected failed shard')
            return original(**kwargs)
        store.put_object = fail
        with self.assertRaises(RuntimeError): S.publish_network(store, 'test', now=NOW+timedelta(minutes=1))
        self.assertEqual(store.data[S.KEY], old)

    def test_consumer_alias_receipt_and_direct_self_exclusion(self):
        store, first = self.build()
        key = SOURCES['prospective-outcomes']['key']
        receipt = C.consume(store, 'test', 'prospective-evaluator', now=NOW)
        self.assertNotIn('prospective-outcomes', receipt['source_contexts'])
        self.assertNotIn('prospective-outcomes', receipt['specialists']['evaluation']['sources'])
        self.assertNotIn('prospective-outcomes', receipt['specialists']['evaluation']['missing'])
        store.data[key] = N.canonical({'generated_at': NOW.isoformat(), 'research_network': receipt})
        second = S.publish_network(store, 'test', now=NOW+timedelta(minutes=1))
        self.assertTrue(second['consumer_processing']['prospective-evaluator']['processed_previous_publication'])
        receipt.update(status='unavailable', publication_id=second['publication_id'])
        store.data[key] = N.canonical({'research_network': receipt})
        third = S.publish_network(store, 'test', now=NOW+timedelta(minutes=2))
        self.assertFalse(third['consumer_processing']['prospective-evaluator']['processed_previous_publication'])

    def test_retained_original_identity_survives_gzip_runtime_header_changes(self):
        store = Store(); raw = b'{"measured":0}'
        encoded = gzip.compress(raw, mtime=0)
        another = bytearray(encoded); another[9] = 3 if encoded[9] != 3 else 255
        S.immutable(store, 'test', 'data/original', bytes(another), compressed=True)
        S.immutable(store, 'test', 'data/original', encoded, compressed=True)
        with self.assertRaises(ValueError):
            S.immutable(store, 'test', 'data/original', gzip.compress(b'{"measured":1}'), compressed=True)

    def test_shard_integrity_and_private_redirect_rejected(self):
        store, manifest = self.build(); shard = next(iter(manifest['shards'].values()))
        store.data[shard['key']] += b' '
        with self.assertRaises(ValueError): S.read_entity(store, 'test', manifest, 'listed_security:NVDA')
        shard['key'] = 'portfolio/snapshot.json'
        with self.assertRaises(ValueError): S.read_entity(store, 'test', manifest, 'listed_security:NVDA')
        self.assertNotIn('portfolio/snapshot.json', store.reads)

    def test_private_context_stays_bound_to_exact_holdings_and_preserves_policy(self):
        store, manifest = self.build(); before = list(store.writes)
        positions = [{'symbol': 'NVDA', 'asset_class': 'equity', 'qty': 3}, {'symbol': 'BTC'}]
        payload = {'capital_decision': 'DATA_HOLD', 'exposure_cap_pct': 0}
        C.attach(payload, store, 'test', 'portfolio-risk', positions=positions, now=NOW)
        context = payload['research_network']
        self.assertEqual(context['access'], 'PRIVATE_OWNER')
        self.assertEqual(context['positions'][1]['status'], 'identity_unresolved')
        self.assertEqual(context['holdings_input_sha256'], hashlib.sha256(N.canonical(positions)).hexdigest())
        self.assertEqual(payload['capital_decision'], 'DATA_HOLD'); self.assertEqual(payload['exposure_cap_pct'], 0)
        self.assertEqual(store.writes, before)

    def test_stale_context_and_authority_escalation_do_not_change_consumer(self):
        store, manifest = self.build()
        self.assertEqual(C.consume(store, 'test', 'engine-fusion', now=NOW+timedelta(days=2))['status'], 'unavailable')
        manifest['calls_eligible'] = True; store.data[S.KEY] = N.canonical(manifest)
        self.assertEqual(C.consume(store, 'test', 'engine-fusion', now=NOW)['status'], 'unavailable')

    def test_unknown_holdings_cannot_become_an_empty_public_portfolio(self):
        store, _ = self.build()
        for value in (None, {}, 'NVDA', False):
            context = C.consume(store, 'test', 'portfolio-risk', positions=value, now=NOW)
            self.assertEqual(context['status'], 'unavailable')
            self.assertEqual(context['access'], 'PRIVATE_OWNER')
            self.assertNotIn('holdings_input_sha256', context)

    def test_prospective_context_requires_actual_pre_registration_storage(self):
        store, manifest = self.build()
        self.assertEqual(P.registration_context(store, 'test', NOW)['status'], 'available_at_registration')
        store.stored = NOW+timedelta(seconds=1)
        self.assertEqual(P.registration_context(store, 'test', NOW)['status'], 'unavailable_at_registration')

    def test_existing_prospective_context_is_never_replaced(self):
        store = Store(); record = {'forecast_id': 'a'*64, 'registered_at': NOW.isoformat(), 'observation': {'instrument': {'symbol': 'NVDA'}}}
        first = P.retain_context(store, 'test', record, {'status': 'unavailable_at_registration'}, 'data/test/')
        self.assertEqual(first['status'], 'retained'); before = dict(store.data)
        second = P.retain_context(store, 'test', record, {'status': 'available_at_registration'}, 'data/test/')
        self.assertEqual(second['status'], 'context_not_retained'); self.assertEqual(before, store.data)


class EventTests(unittest.TestCase):
    def test_all_events_including_after_ten_are_attempted(self):
        calls = []
        def put(**kwargs):
            calls.append(kwargs['Entries']); return {'Entries': [{'EventId': str(i)} for i in range(len(kwargs['Entries']))]}
        result = publish_many([('test', {})]*23, client=types.SimpleNamespace(put_events=put))
        self.assertEqual([len(c) for c in calls], [10, 10, 3]); self.assertEqual(result['n_published'], 23)

    def test_partial_and_missing_acknowledgements_are_failures(self):
        client = types.SimpleNamespace(put_events=lambda **kw: {'Entries': [{'EventId': 'ok'}, {'ErrorCode': 'Throttling'}]})
        result = publish_many([('test', {})]*3, client=client)
        self.assertEqual(result['n_published'], 1); self.assertEqual(result['n_failed'], 2)
        self.assertFalse(result['results'][2]['ok'])

    def test_empty_batch_does_not_call_aws(self):
        client = types.SimpleNamespace(put_events=lambda **kw: self.fail('unexpected AWS call'))
        self.assertTrue(publish_many([], client=client)['ok'])

    def test_outbox_retries_only_unacknowledged_event_with_same_id(self):
        store = Store(); calls = []
        def partial(events):
            calls.append(events); return {'results': [{'index': i, 'ok': i == 0} for i in range(len(events))]}
        events = [('tier', {'ticker': 'A'}, 'ranker'), ('tier', {'ticker': 'B'}, 'ranker')]
        result = advance(store, 'test', 'data/checkpoint', {}, None, {'run': 1}, events, partial)
        self.assertEqual(result['pending'], 1)
        state, etag = read_state(store, 'test', 'data/checkpoint')
        self.assertEqual(state['checkpoint'], {'run': 1})
        advance(store, 'test', 'data/checkpoint', state, etag, {'run': 2}, [], partial)
        self.assertEqual(len(calls[1]), 1)
        self.assertEqual(calls[0][1][1]['_event_id'], calls[1][0][1]['_event_id'])
        self.assertEqual(read_state(store, 'test', 'data/checkpoint')[0]['pending'], [])

    def test_checkpoint_failure_prevents_emission_and_stale_writer_cannot_overwrite(self):
        store = Store(); store.data['data/checkpoint'] = b'{}'
        with self.assertRaises(Conflict):
            advance(store, 'test', 'data/checkpoint', {}, None, {}, [('test', {}, 'test')], lambda _: self.fail('emitted before persistence'))

    def test_ack_write_failure_retains_pending_events(self):
        store = Store(); put = store.put_object
        def fail_ack(**kwargs):
            if kwargs.get('IfMatch'): raise RuntimeError('ack save failure')
            return put(**kwargs)
        store.put_object = fail_ack
        with self.assertRaises(RuntimeError):
            advance(store, 'test', 'data/checkpoint', {}, None, {}, [('test', {}, 'test')], lambda _: {'results': [{'index': 0, 'ok': True}]})
        self.assertEqual(len(read_state(store, 'test', 'data/checkpoint')[0]['pending']), 1)


class FirmBookTests(unittest.TestCase):
    def test_real_pair_object_shape_and_legacy_strings(self):
        path = ROOT/'aws/lambdas/justhodl-firm-book/source/lambda_function.py'
        spec = importlib.util.spec_from_file_location('firm_book_network_test', path)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: object())}):
            spec.loader.exec_module(module)
        module.get_json = lambda _: {'pairs': [{'long_leg': {'symbol': 'BAC', 'name': 'Bank of America', 'price': 54.09}, 'short_leg': 'GS', 'score': 2}]}
        rows, _ = module.extract_desk('pairs-arb', module.DESK_SPECS['pairs-arb'])
        self.assertEqual([r['symbol'] for r in rows], ['BAC', 'GS'])
        self.assertEqual([r['side'] for r in rows], [1, -1]); self.assertEqual(rows[0]['price'], 54.09)
        self.assertIsNone(module.position_identity("{'SYMBOL': 'BAC'}"))


if __name__ == '__main__': unittest.main()
