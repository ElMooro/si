"""Actual original collection, retained replay and conditional publication tests."""
from pathlib import Path
from unittest import mock
import copy, io, json, sys, time, urllib.error, unittest
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(Path(__file__).parent)]
import provider_flow_collect as collect
import provider_flow_store as store
import provider_flow_native as native
import provider_flow_model as model
import provider_flow_catalog as catalog
from test_provider_flow_research import fixtures, retain, STAMP


class ObjectError(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Storage:
    def __init__(self): self.objects = {}; self.writes = []; self.reads = []
    def get_object(self, **kw):
        key = kw['Key']; self.reads.append(key)
        if key not in self.objects: raise ObjectError('NoSuchKey')
        raw = self.objects[key]
        return {'Body': io.BytesIO(raw), 'ETag': native.sha(raw)}
    def put_object(self, **kw):
        key = kw['Key']; old = self.objects.get(key)
        if kw.get('IfNoneMatch') == '*' and old is not None: raise ObjectError('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or native.sha(old) != kw['IfMatch']): raise ObjectError('PreconditionFailed')
        self.objects[key] = kw['Body']; self.writes.append(key)


def source_fixture(storage):
    collections = {}
    for ticker in catalog.ETF_UNIVERSE:
        collection, raw = retain(ticker, fixtures(ticker)); collections[ticker] = collection; storage.objects.update(raw)
    contexts = {}
    prior = native.encoded({'generated_at': '2026-09-20T22:00:00+00:00', 'arbitrary_complete_predecessor': [1, None, 0]})
    for key in store.CONTEXTS:
        storage.objects[key] = prior; contexts[key] = store.snapshot(storage, 'b', key)
    storage.objects['etf-flows/history/2026-09-20.json'] = prior
    return {'contract': 'provider-flow-inputs.v1', 'kind': 'flow', 'generated_at': STAMP,
            'collections': collections, 'contexts': contexts, 'previous': None, 'provider_requests': 300}


class OriginalCollection(unittest.TestCase):
    def setUp(self):
        self.saved = {}
        def save(raw):
            digest = native.sha(raw); key = native.PRIVATE + digest + '.bin'; self.saved[key] = raw
            return {'key': key, 'sha256': digest, 'bytes': len(raw)}
        self.collector = collect.Collector('fixture-provider-secret', save, time.monotonic() + 1000)
        self.collector.query_date = '2026-09-21'

    def test_actual_reader_header_auth_and_whole_response_retention(self):
        raw = native.encoded({'status': 'OK', 'results': fixtures(), 'future_extension': {'preserved': True}})
        opener = mock.Mock(); opener.open.return_value = io.BytesIO(raw)
        with mock.patch.object(collect.urllib.request, 'build_opener', return_value=opener), mock.patch.object(collect, 'now', return_value=STAMP):
            collection = self.collector.fund('SPY')
        req = opener.open.call_args.args[0]
        self.assertEqual(req.get_header('Authorization'), 'Bearer fixture-provider-secret')
        self.assertNotIn('fixture-provider-secret', req.full_url)
        self.assertEqual(list(self.saved.values()), [raw]); self.assertEqual(self.collector.requests, 1)
        self.assertEqual(collection['status'], 'retained')
        self.assertNotIn('fixture-provider-secret', json.dumps(collection))
        self.assertEqual(native.reconstruct('SPY', collection, self.saved.__getitem__, STAMP)['quality']['observed_dates'], 25)

    def test_error_body_and_exception_text_are_never_published(self):
        class ForbiddenBody:
            def read(self, *a): raise AssertionError('Error body must not be read')
            def close(self): pass
        error = urllib.error.HTTPError('https://provider.invalid/?apiKey=secret', 403, 'private-detail', {}, ForbiddenBody())
        opener = mock.Mock(); opener.open.side_effect = error
        with mock.patch.object(collect.urllib.request, 'build_opener', return_value=opener): result = self.collector.fund('SPY')
        self.assertEqual(result['status'], 'provider_http_error'); self.assertEqual(result['http_status'], 403)
        self.assertNotIn('private-detail', json.dumps(result)); self.assertEqual(self.saved, {})

    def test_credential_echo_deadline_and_redirect_do_not_capture(self):
        opener = mock.Mock(); opener.open.return_value = io.BytesIO(b'{"echo":"fixture-provider-secret"}')
        with mock.patch.object(collect.urllib.request, 'build_opener', return_value=opener): result = self.collector.fund('SPY')
        self.assertEqual(result['status'], 'response_rejected'); self.assertEqual(self.saved, {})
        self.collector.deadline = time.monotonic() - 1
        with mock.patch.object(collect.urllib.request, 'build_opener', side_effect=AssertionError('Must not request')):
            self.assertEqual(self.collector.fund('XLF')['status'], 'acquisition_deadline')
        with self.assertRaises(ValueError): collect.NoRedirect().redirect_request(None, None, None, None, None, None)

    def test_pagination_chain_and_schema_rejections_are_retained_without_truncation(self):
        following = native.ENDPOINT + '?cursor=page-two'
        rows = fixtures(); bodies = [native.encoded({'status': 'OK', 'results': rows[:10], 'next_url': following}),
                                    native.encoded({'status': 'OK', 'results': rows[10:]})]
        with mock.patch.object(self.collector, 'response', side_effect=[(raw, None, None) for raw in bodies]), mock.patch.object(collect, 'now', return_value=STAMP):
            result = self.collector.fund('SPY')
        self.assertEqual(result['status'], 'retained'); self.assertEqual(len(result['pages']), 2)
        self.assertEqual(len(native.reconstruct('SPY', result, self.saved.__getitem__, STAMP)['history']), 25)
        bad = native.encoded({'status': 'OK', 'results': [], 'next_url': 'https://foreign.invalid/?cursor=x'})
        with mock.patch.object(self.collector, 'response', return_value=(bad, None, None)):
            result = self.collector.fund('XLF')
        self.assertEqual(result['status'], 'response_rejected'); self.assertEqual(result['pages'], [])
        self.assertEqual(self.saved[result['rejected_original']['key']], bad)

    def test_storage_failure_is_not_a_provider_failure(self):
        raw = native.encoded({'status': 'OK', 'results': []})
        self.collector.save = mock.Mock(side_effect=RuntimeError('storage failed'))
        with mock.patch.object(self.collector, 'response', return_value=(raw, None, None)), self.assertRaises(RuntimeError):
            self.collector.fund('SPY')


class RetainedFlow(unittest.TestCase):
    def test_original_replay_and_radar_require_all_matching_evidence(self):
        storage = Storage(); inputs = source_fixture(storage); read = store.reader(storage, 'b')
        output, histories = store.compile_output(inputs, read)
        ref = store.retain(storage, 'b', inputs, output, histories)
        self.assertEqual(store.replay(ref, store.reader(storage, 'b')), output)
        packet = {**output, 'replay': ref}
        self.assertTrue(store.conditional(storage, 'b', model.CURRENT, packet))
        radar_inputs = {'contract': 'capital-radar-inputs.v1', 'kind': 'radar', 'generated_at': STAMP,
                        'canonical_source': store.snapshot(storage, 'b', model.CURRENT), 'previous': None}
        radar, radar_histories = store.compile_output(radar_inputs, store.reader(storage, 'b'))
        radar_ref = store.retain(storage, 'b', radar_inputs, radar, radar_histories)
        self.assertEqual(store.replay(radar_ref, store.reader(storage, 'b')), radar)
        self.assertEqual(radar['complexes'], output['complexes']); self.assertEqual(radar['canonical_replay'], ref)
        key = inputs['collections']['SPY']['pages'][0]['original']['key']; original = storage.objects[key]
        storage.objects[key] += b' '
        with self.assertRaises(ValueError): store.replay(ref, store.reader(storage, 'b'))
        storage.objects[key] = original
        history = output['funds']['SPY']['history']['key']; storage.objects[history] += b' '
        with self.assertRaises(ValueError): store.replay(ref, store.reader(storage, 'b'))

    def test_current_and_old_vintages_cannot_replace_newer_publication(self):
        storage = Storage(); base = {'contract': model.CONTRACT, 'generated_at': STAMP,
            'funds': {'SPY': {'latest_effective_date': '2026-09-18'}}}
        self.assertTrue(store.conditional(storage, 'b', model.CURRENT, base))
        old = {**base, 'generated_at': '2026-09-21T05:59:00+00:00'}
        self.assertFalse(store.conditional(storage, 'b', model.CURRENT, old))
        regressed = {**base, 'generated_at': '2026-09-21T06:01:00+00:00',
                     'funds': {'SPY': {'latest_effective_date': '2026-09-17'}}}
        self.assertFalse(store.conditional(storage, 'b', model.CURRENT, regressed))
        with self.assertRaises(ValueError): store.conditional(storage, 'b', model.CURRENT, {**base, 'different': True})
        self.assertEqual(json.loads(storage.objects[model.CURRENT]), base)

    def test_durable_producer_request_does_not_repeat_collection(self):
        storage = Storage(); prepared = source_fixture(storage)
        fake = mock.Mock(); fake.collect.return_value = (prepared['collections'], 300)
        with mock.patch.object(store.collector, 'Collector', return_value=fake), mock.patch.object(store, 'now', return_value=STAMP):
            first = store.run(storage, 'b', 'flow', 'fixture-public-flow', 'fixture-execution')
            second = store.run(storage, 'b', 'flow', 'fixture-public-flow', 'different-execution')
        self.assertTrue(first['published']); self.assertEqual(first, second); self.assertEqual(fake.collect.call_count, 1)
        self.assertTrue(store.MIGRATION in storage.objects)
        self.assertEqual(first['signals_emitted'], 0); self.assertEqual(first['notifications_sent'], 0)
        self.assertTrue(all(store.artifact(k) or store.source_key(k) or k == store.MIGRATION or '/requests/' in k for k in storage.reads))

    def test_reader_rejects_nonresearch_keys_and_compiler_tampering(self):
        storage = Storage(); read = store.reader(storage, 'b')
        for key in ('portfolio/current.json', 'config/credentials.json', native.PRIVATE + '../other.bin'):
            with self.assertRaises(ValueError): read(key)
        self.assertEqual(storage.reads, [])
        inputs = source_fixture(storage); output, histories = store.compile_output(inputs, store.reader(storage, 'b'))
        ref = store.retain(storage, 'b', inputs, output, histories)
        run = json.loads(storage.objects[ref['manifest_key']]); key = run['compilers']['provider_flow_native']['key']
        storage.objects[key] = b'raise RuntimeError("unreviewed compiler must not execute")'
        with self.assertRaises(ValueError): store.replay(ref, store.reader(storage, 'b'))


if __name__ == '__main__': unittest.main(verbosity=2)
