"""Isolated full native Port Cargo runs; never call a provider or AWS."""
from pathlib import Path
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from unittest.mock import patch
from copy import deepcopy
import ast, contextlib, hashlib, importlib.util, json, sys, unittest, urllib.error, urllib.parse

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1]/'source'
sys.path[:0] = [str(SOURCE), str(ROOT/'aws/shared')]
import cargo_store as store
NOW = datetime(2026, 9, 27, 12, 40, tzinfo=timezone.utc)
with patch('boto3.client'):
    spec = importlib.util.spec_from_file_location('cargo_native_test', SOURCE/'lambda_function.py')
    native = importlib.util.module_from_spec(spec); sys.modules[spec.name] = native
    spec.loader.exec_module(native)

class Missing(Exception): response = {'Error': {'Code': 'NoSuchKey'}}
class Denied(Exception): response = {'Error': {'Code': 'AccessDenied'}}
class Conflict(Exception): response = {'Error': {'Code': 'PreconditionFailed'}}


class Memory:
    def __init__(self):
        self.data = {}; self.writes = []; self.reads = []
        self.error = self.truncated = self.conflict = self.corrupt = None
    def get_object(self, **kw):
        key = kw['Key']; self.reads.append(key)
        if key == self.error: raise Denied()
        if key not in self.data: raise Missing()
        raw = self.data[key]
        if key == self.corrupt: raw += b'!'
        return {'Body': BytesIO(raw), 'ContentLength': len(raw) + (key == self.truncated),
                'ETag': store.sha(raw), 'LastModified': NOW}
    def put_object(self, **kw):
        key, raw = kw['Key'], kw['Body']
        if key.startswith(store.PRIVATE):
            assert kw['IfNoneMatch'] == '*'
            if key in self.data: raise Conflict()
        else:
            assert key in (store.HEAD, store.CHOICE) and 'IfMatch' in kw
            if key == self.conflict:
                self.data[key] = b'newer-writer'; raise Conflict()
            if kw['IfMatch'] != store.sha(self.data[key]): raise Conflict()
        self.data[key] = raw; self.writes.append(key)
        return {}


def fixture():
    memory = Memory()
    memory.data = {
        store.HEAD: store.encode({'version': '1.3.0', 'generated_at': '2026-09-26T12:40:00Z', 'all_original_fields': list(range(100))}),
        store.CHOICE: store.encode({'layer_url': native.LAYER, 'datefield': 'date', 'service': 'Daily_Ports_Data'}),
        store.PORTWATCH: store.encode({'ports': [{'name': 'Port '+str(i), 'industry_exposure': ['Fixture industry']} for i in range(3)], 'chokepoints': [{'name': 'Fixture channel'}]}),
        store.GRAPH: store.encode({'sector_etf_proxy': {'Fixture sector': 'FIX'}}),
        store.BETAS: store.encode({'betas': {'port_throughput_pulse': {'Fixture sector': {'beta': 0.5, 'n_obs': 12, 'se': 0.2}}}}),
    }
    rows = []
    for p in range(3):
        for i in range(42):
            rows.append({'ObjectId': len(rows)+1, 'portid': 'p'+str(p), 'portname': 'Port '+str(p),
                         'country': 'Fixture country', 'ISO3': 'FIX', 'date': (NOW-timedelta(days=44-i)).date().isoformat(),
                         'import': 10000 + i*100, 'export': 7000 + i*100, 'import_container': 9000})
    calls = []
    def opener(req, timeout=None):
        calls.append(req.full_url)
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(req.full_url).query))
        if params.get('returnCountOnly') == 'true': result = {'count': len(rows)}
        elif 'outStatistics' in params:
            value = {'imp': 1000000, 'exp': 800000}
            if 'groupByFieldsForStatistics' in params: value['country'] = 'Fixture country'
            result = {'features': [{'attributes': value}]}
        elif params.get('resultRecordCount') == '1': result = {'features': [{'attributes': rows[-1]}]}
        else: result = {'features': [{'attributes': r} for r in rows], 'exceededTransferLimit': False}
        raw = store.encode(result)
        return store.Response(raw, headers={'Content-Length': str(len(raw))})
    return memory, rows, calls, opener


class Tests(unittest.TestCase):
    def execute(self, memory, opener):
        native.s3 = memory
        with contextlib.redirect_stdout(StringIO()):
            return store.run(native, opener=opener, at=NOW.isoformat())

    def test_complete_inputs_http_native_calculation_and_projection_replay(self):
        memory, rows, calls, opener = fixture(); previous = dict(memory.data)
        self.assertTrue(self.execute(memory, opener)['published'])
        packet = store.decode(memory.data[store.HEAD]); count = len(calls)
        self.assertEqual(packet['n_rows_window'], 126); self.assertEqual(packet['n_ports_with_data'], 3)
        self.assertFalse(packet['sizing_eligible']); self.assertEqual(packet['portfolio_action'], 'WAIT')
        manifest = store.decode(store.retained(memory, native.BUCKET, packet['publication_context']['manifest']))
        self.assertEqual(set(manifest['inputs']), set(store.KEYS)); self.assertEqual(manifest['read_order'], list(store.READS))
        for key, raw in previous.items():
            self.assertEqual(store.retained(memory, native.BUCKET, manifest['inputs'][key]['original']), raw)
        self.assertEqual(set(manifest['compiler_sha256']), set(store.COMPILERS))
        self.assertEqual(len(manifest['http_attempts']), 10)
        with contextlib.redirect_stdout(StringIO()): replay = store.replay(native, memory, native.BUCKET, packet)
        self.assertEqual(replay['original_native_rows'], 126); self.assertEqual(len(calls), count)
        self.assertEqual([k for k in memory.writes if not k.startswith(store.PRIVATE)], [store.HEAD])

    def test_predecessor_denial_missing_truncation_corruption_preserves_old_publication(self):
        for case in ('denied', 'missing', 'truncated', 'malformed'):
            memory, _, calls, opener = fixture(); original = memory.data[store.HEAD]
            if case == 'denied': memory.error = store.GRAPH
            if case == 'missing': del memory.data[store.BETAS]
            if case == 'truncated': memory.truncated = store.PORTWATCH
            if case == 'malformed': memory.data[store.CHOICE] = b'{broken'
            with self.assertRaises(Exception): self.execute(memory, opener)
            self.assertEqual(calls, []); self.assertEqual(memory.data[store.HEAD], original)
            self.assertTrue(all(k.startswith(store.PRIVATE) for k in memory.writes))

    def test_whole_http_failure_body_retained_but_cannot_publish(self):
        memory, _, _, _ = fixture(); before = memory.data[store.HEAD]; raw = b'complete provider rate limit body'
        def limited(*a, **kw): raise urllib.error.HTTPError(native.LAYER, 429, 'rate limited', {'Content-Length': str(len(raw))}, BytesIO(raw))
        with self.assertRaises(store.CaptureError): self.execute(memory, limited)
        self.assertIn(raw, memory.data.values()); self.assertEqual(memory.data[store.HEAD], before)

    def test_http_200_provider_error_or_malformed_response_does_not_become_zero(self):
        for raw in (b'{"error":{"code":400}}', b'{malformed', b'{"features":[],"features":[]}'):
            memory, _, _, _ = fixture(); before = memory.data[store.HEAD]
            with self.assertRaises(store.CaptureError):
                self.execute(memory, lambda *a, **kw: store.Response(raw))
            self.assertIn(raw, memory.data.values()); self.assertEqual(memory.data[store.HEAD], before)

    def test_transport_attempt_is_retained_without_retry_or_empty_publish(self):
        memory, _, _, _ = fixture(); attempts = []
        def denied(*a, **kw): attempts.append(1); raise urllib.error.URLError('fixture failure')
        with self.assertRaises(store.CaptureError): self.execute(memory, denied)
        self.assertEqual(len(attempts), 1)
        self.assertTrue(any(b'transport_error' in v for v in memory.data.values()))
        self.assertTrue(all(k.startswith(store.PRIVATE) for k in memory.writes))

    def test_input_cache_and_native_global_state_are_restored(self):
        memory, _, _, opener = fixture()
        module = native.impact_mapper; cache = {'graph': {'wrong': True}, 'betas': {'wrong': True}}
        old = (module._S3, module._CACHE, module.datetime)
        module._CACHE = cache
        try:
            original = {k: getattr(native, k) for k in ('datetime', 'time', 'urllib', 'LAYER', 'DATEFIELD', 'RESOLVER_PATH')}
            self.assertTrue(self.execute(memory, opener)['published'])
            self.assertIs(module._CACHE, cache); self.assertEqual(module._CACHE, {'graph': {'wrong': True}, 'betas': {'wrong': True}})
            for key, value in original.items(): self.assertIs(getattr(native, key), value)
            self.assertIs(module._S3, old[0]); self.assertIs(module.datetime, old[2])
        finally: module._S3, module._CACHE, module.datetime = old

    def test_concurrent_writer_is_preserved_without_rollback(self):
        memory, _, _, opener = fixture(); memory.conflict = store.HEAD
        result = self.execute(memory, opener)
        self.assertFalse(result['published']); self.assertEqual(result['completed_paths'], [])
        self.assertEqual(memory.data[store.HEAD], b'newer-writer')

    def test_resolved_choice_is_deferred_and_each_partial_publication_is_explicit(self):
        for conflict in (None, store.CHOICE, store.HEAD):
            memory, _, _, opener = fixture()
            memory.data[store.CHOICE] = b'{}'; previous = memory.data[store.HEAD]; memory.conflict = conflict
            result = self.execute(memory, opener)
            if conflict is None:
                self.assertTrue(result['published'])
                packet = store.decode(memory.data[store.HEAD])
                with contextlib.redirect_stdout(StringIO()): store.replay(native, memory, native.BUCKET, packet)
                self.assertEqual([k for k in memory.writes if not k.startswith(store.PRIVATE)], [store.CHOICE, store.HEAD])
            elif conflict == store.CHOICE:
                self.assertFalse(result['published']); self.assertEqual(result['completed_paths'], [])
                self.assertEqual(memory.data[store.HEAD], previous)
            else:
                self.assertFalse(result['published']); self.assertEqual(result['completed_paths'], [store.CHOICE])
                self.assertEqual(memory.data[store.HEAD], b'newer-writer')

    def test_failure_after_resolving_choice_never_writes_it_early(self):
        memory, _, calls, opener = fixture(); memory.data[store.CHOICE] = b'{}'
        def fail_later(req, timeout=None):
            if len(calls) > 2: raise urllib.error.URLError('later input failed')
            return opener(req, timeout=timeout)
        with self.assertRaises(store.CaptureError): self.execute(memory, fail_later)
        self.assertEqual(memory.data[store.CHOICE], b'{}')
        self.assertTrue(all(k.startswith(store.PRIVATE) for k in memory.writes))

    def test_ragged_original_rows_stay_retained_and_legacy_limitation_is_explicit(self):
        memory, rows, _, opener = fixture()
        rows.append({**rows[-1], 'ObjectId': 127, 'date': (NOW-timedelta(days=2)).date().isoformat()})
        self.assertTrue(self.execute(memory, opener)['published'])
        packet = store.decode(memory.data[store.HEAD]); self.assertEqual(packet['ragged_days_trimmed'], 1)
        self.assertEqual(packet['n_rows_window'], 127); self.assertFalse(packet['forecast_qualified'])
        with contextlib.redirect_stdout(StringIO()): store.replay(native, memory, native.BUCKET, packet)

    def test_malformed_or_credential_bearing_cached_url_never_leaves_session(self):
        for url in ('https://example.invalid/query?f=pjson', native.LAYER+'?token=secret', native.LAYER.replace('https://', 'https://secret@')):
            memory, _, calls, opener = fixture(); memory.data[store.CHOICE] = store.encode({'layer_url': url})
            with self.assertRaises(store.CaptureError): self.execute(memory, opener)
            self.assertEqual(calls, [])

    def test_replay_refuses_changed_public_output_or_retained_body(self):
        memory, _, _, opener = fixture(); self.execute(memory, opener); packet = store.decode(memory.data[store.HEAD])
        mutated = deepcopy(packet); mutated['global_pulse']['total_chg_pct'] = 999
        with self.assertRaises(store.CaptureError), contextlib.redirect_stdout(StringIO()): store.replay(native, memory, native.BUCKET, mutated)
        mutated = deepcopy(packet); mutated['publication_context']['capture_elapsed_s'] += 1
        with self.assertRaises(store.CaptureError): store.replay(native, memory, native.BUCKET, mutated)
        manifest = store.decode(store.retained(memory, native.BUCKET, packet['publication_context']['manifest']))
        memory.corrupt = manifest['http_attempts'][0]['original']['key']
        with self.assertRaises(store.CaptureError), contextlib.redirect_stdout(StringIO()): store.replay(native, memory, native.BUCKET, packet)

    def test_complete_original_source_and_unchanged_calculations_are_preserved(self):
        raw = (ROOT/'tests/fixtures/pre-shipping-qualification-port-cargo.py.txt').read_bytes()
        self.assertEqual(store.sha(raw), 'f5d32c04ea2c204e873464c8b8edec2df92efcad7a51de78adc66bec5ee717fc')
        original = ast.parse(raw); current = ast.parse((SOURCE/'lambda_function.py').read_bytes())
        old = {n.name: ast.dump(n, include_attributes=False) for n in original.body if isinstance(n, ast.FunctionDef)}
        new = {n.name: n for n in current.body if isinstance(n, ast.FunctionDef)}
        for name, tree in old.items():
            node = deepcopy(new['_native_calculation' if name == 'lambda_handler' else name])
            if name == 'lambda_handler':
                node.name = name
                for item in ast.walk(node):
                    if isinstance(item, ast.Constant) and item.value == '1.3.1': item.value = '1.3.0'
            self.assertEqual(ast.dump(node, include_attributes=False), tree, name)

    def test_budget_exhaustion_and_retention_failure_never_publish(self):
        memory, _, calls, opener = fixture()
        with patch.object(store.time, 'monotonic', side_effect=[0, 601]), self.assertRaises(store.CaptureError): self.execute(memory, opener)
        self.assertEqual(calls, [])
        memory, _, calls, opener = fixture()
        with patch.object(store, 'retain', side_effect=store.CaptureError('fixture storage denied')), self.assertRaises(store.CaptureError): self.execute(memory, opener)
        self.assertEqual(calls, [])

    def test_duplicate_nonfinite_and_oversized_bodies_refused(self):
        for raw in (b'{"x":NaN}', b'{"x":1e400}', b'{"x":1,"x":2}'):
            with self.assertRaises(store.CaptureError): store.strict(raw)
        with patch.object(store, 'LIMIT', 2), self.assertRaises(store.CaptureError): store.whole(BytesIO(b'123'))


if __name__ == '__main__': unittest.main(verbosity=2)
