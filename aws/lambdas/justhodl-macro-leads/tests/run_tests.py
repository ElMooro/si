"""Whole native calculation, actual GPR workbook and failure-safe replay tests."""
from pathlib import Path
from copy import deepcopy
from io import BytesIO
from types import ModuleType
from unittest.mock import patch
from decimal import Decimal
import ast, gzip, importlib.util, json, sys, unittest, urllib.error, urllib.parse

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1] / 'source'
sys.path.insert(0, str(SOURCE))
import macro_store as store
import macro_measurements as m
AT = '2026-09-27T12:20:00+00:00'
GPR_SHA = '6ff388db4aa34eb66433d4c13c9f82d88316714889a1fc9e6a8c0ce1158d5eae'


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self):
        self.data = {store.HEAD: store.encode({'generated_at': '2026-09-26T12:20:00Z', 'version': 'legacy',
            'unknown_original': [0, False, None, {'keep': 'all'}], 'heavy_truck_sales': {'old_nested': 42}})}
        self.reads = []; self.writes = []; self.denied = set(); self.truncated = set(); self.race = None
    def get_object(self, **kw):
        key = kw['Key']; self.reads.append(key)
        if key in self.denied: raise Error('AccessDenied')
        if key not in self.data: raise Error('NoSuchKey')
        raw = self.data[key]
        return {'Body': BytesIO(raw), 'ContentLength': len(raw) + (key in self.truncated), 'ETag': store.sha(raw)}
    def put_object(self, **kw):
        key = kw['Key']; raw = kw['Body']
        if self.race: self.race(key)
        if key in self.denied: raise Error('AccessDenied')
        old = self.data.get(key)
        if kw.get('IfNoneMatch') == '*' and old is not None: raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or kw['IfMatch'] != store.sha(old)): raise Error('PreconditionFailed')
        self.data[key] = raw; self.writes.append(key)


class Response(BytesIO):
    def __init__(self, raw, url, status=200):
        super().__init__(raw); self.url = url; self.status = status
        self.headers = {'Content-Length': str(len(raw)), 'Content-Type': 'application/octet-stream'}
    def getcode(self): return self.status
    def geturl(self): return self.url


def truck_packets():
    meta = {'seriess': [{'id': 'HTRUCKSSAAR', 'units': 'Millions of Units', 'frequency_short': 'M',
                        'seasonal_adjustment_short': 'SAAR'}]}
    rows = [{'date': m.shift('1985-01', i) + '-01', 'value': str(0.2 + i / 2000),
             'realtime_start': AT[:10], 'realtime_end': AT[:10]} for i in range(500)]
    return meta, {'count': len(rows), 'offset': 0, 'units': 'lin', 'output_type': 1, 'observations': rows}


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workbook = gzip.decompress((ROOT / 'tests/fixtures/gpr-original-20260927.xls.gz').read_bytes())
        assert store.sha(cls.workbook) == GPR_SHA
        cls.module = None
        fake = ModuleType('boto3'); fake.client = lambda *a, **k: None
        spec = importlib.util.spec_from_file_location('macro_native_test', SOURCE / 'lambda_function.py')
        cls.module = importlib.util.module_from_spec(spec); sys.modules[spec.name] = cls.module
        with patch.dict(sys.modules, {'boto3': fake}): spec.loader.exec_module(cls.module)
    def source(self, req, timeout):
        url = req if isinstance(req, str) else req.full_url
        self.calls.append(url); p = urllib.parse.urlsplit(url); q = dict(urllib.parse.parse_qsl(p.query))
        if p.netloc == 'api.stlouisfed.org':
            meta, obs = truck_packets()
            if p.path.endswith('/series'): raw = store.encode(meta)
            else:
                if q.get('sort_order') == 'desc':
                    rows = obs['observations'][-int(q['limit']):][::-1]
                    obs = {**obs, 'count': len(rows), 'observations': rows}
                raw = store.encode(obs)
        elif p.netloc == 'query1.finance.yahoo.com':
            raw = store.encode({'chart': {'result': [{'timestamp': [1700000000 + 86400*i for i in range(40)],
                'indicators': {'quote': [{'close': [100 + i for i in range(40)]}]}}]}})
        elif url == store.GPR: raw = self.workbook
        else: raise AssertionError('Undeclared source URL')
        return Response(raw, url)
    def execute(self, mem=None, opener=None, at=AT):
        mem = mem or Memory(); self.calls = []; self.module.S3 = mem; self.module.FRED_KEY = 'fixture-secret-not-real'
        result = store.run(self.module, at=at, opener=opener or self.source)
        return mem, store.strict(mem.data[store.HEAD]), result
    def test_original_functions_remain_whole_and_entry_delegates(self):
        old = (ROOT / 'tests/fixtures/pre-freight-research-macro-leads.py.txt').read_bytes()
        self.assertEqual(store.sha(old), '8de2b9eb82b35f78d44e20fe6cc18c3eb4177f2c47dc76cd9b2a90abd945b94f')
        before = {n.name: n for n in ast.parse(old).body if isinstance(n, ast.FunctionDef)}
        after = {n.name: n for n in ast.parse((SOURCE / 'lambda_function.py').read_bytes()).body if isinstance(n, ast.FunctionDef)}
        for name, node in before.items():
            other = deepcopy(after['_legacy_lambda_handler' if name == 'lambda_handler' else name]); other.name = name
            self.assertEqual(ast.dump(node), ast.dump(other), name)
        with patch.object(store, 'run', return_value={'delegated': True}) as run:
            self.assertEqual(self.module.lambda_handler({'check': 1}, None), {'delegated': True})
            self.assertIs(run.call_args.args[0], self.module)
    def test_actual_complete_workbook_calendar_and_prior_60(self):
        out = m.gpr(self.workbook, AT)
        self.assertEqual(out['series_count'], 112); self.assertEqual(out['monthly_rows'], 1520)
        self.assertEqual(out['monthly_positions'], 170240); self.assertEqual(out['latest_month'], '2026-08')
        self.assertEqual(out['level'], 117.9188461303711)
        self.assertEqual(out['prior_60_months']['start'], '2021-08'); self.assertEqual(out['prior_60_months']['end'], '2026-07')
        self.assertAlmostEqual(out['prior_60_months']['z'], -0.5142102604139448)
        self.assertEqual(out['series']['GPR']['values'][:1020], [None]*1020)
        self.assertEqual(len(out['source_labels']), 113); self.assertFalse(out['original_vintage_verified'])
        for series in out['series'].values(): self.assertEqual(len(series['values']), 1520)
    def test_full_native_attempts_whole_sources_and_offline_replay(self):
        mem = Memory(); prior = mem.data[store.HEAD]; mem, packet, result = self.execute(mem)
        self.assertEqual(len(self.calls), 31); self.assertEqual(result['provider_attempts'], 31)
        self.assertGreater(len(mem.data[store.HEAD]), 40000); self.assertIn(prior, mem.data.values()); self.assertIn(self.workbook, mem.data.values())
        self.assertEqual(packet['unknown_original'], [0, False, None, {'keep': 'all'}])
        self.assertEqual(packet['heavy_truck_sales']['old_nested'], 42)
        self.assertEqual(packet['measurement_review']['heavy_truck']['returned_rows'], 500)
        self.assertNotEqual(packet['heavy_truck_sales']['z_1y'], packet['legacy_calculation']['heavy_truck_sales']['z_1y'])
        self.assertEqual(packet['portfolio_action'], 'WAIT'); self.assertEqual(len(packet['compiler_sha256']), 20)
        self.assertEqual(mem.writes.count(store.HEAD), 1)
        for key in m.FLAGS: self.assertIs(packet[key], False)
        self.assertNotIn(b'fixture-secret-not-real', b''.join(mem.data.values()))
        before = list(mem.writes)
        with patch.object(urllib.request, 'urlopen', side_effect=AssertionError('Offline replay')):
            replay = store.replay(self.module, mem, self.module.BUCKET, packet)
        self.assertEqual(replay['gpr_monthly_positions'], 170240); self.assertEqual(replay['truck_observations'], 500)
        self.assertEqual(mem.writes, before); self.assertIs(self.module.S3, mem)
    def test_exact_months_missing_zero_and_current_exclusion(self):
        meta, obs = truck_packets(); result = m.truck(meta, obs, AT)
        self.assertEqual(result['prior_12_months']['start'], '2025-08'); self.assertEqual(result['prior_12_months']['end'], '2026-07')
        self.assertAlmostEqual(result['yoy']['percent'], (0.4495/0.4435-1)*100)
        obs['observations'][-13]['value'] = '.'; result = m.truck(meta, obs, AT)
        self.assertEqual(result['yoy']['status'], 'prior_month_missing'); self.assertEqual(result['prior_12_months']['status'], 'incomplete_baseline')
        obs['observations'][-13]['value'] = '0'; result = m.truck(meta, obs, AT)
        self.assertEqual(result['yoy']['status'], 'zero_denominator')
        obs['observations'][-1]['value'] = '.'; result = m.truck(meta, obs, AT)
        self.assertEqual(result['status'], 'latest_missing'); self.assertIsNone(result['level'])
        values = {m.shift('2026-08', i): Decimal(2) for i in range(-12, 1)}
        self.assertEqual(m.baseline(values, '2026-08', 12)['status'], 'zero_variance')
    def test_unit_identity_population_duplicate_and_future_rejection(self):
        for field, value in (('units', 'Thousands'), ('frequency_short', 'D'), ('seasonal_adjustment_short', 'NSA'), ('id', 'OTHER')):
            meta, obs = truck_packets(); meta['seriess'][0][field] = value
            self.assertNotEqual(m.truck(meta, obs, AT)['status'], 'measured')
        for field, value in (('count', 499), ('offset', True), ('units', 'pc1'), ('output_type', True)):
            meta, obs = truck_packets(); obs[field] = value
            self.assertEqual(m.truck(meta, obs, AT)['status'], 'incomplete_or_transformed_response')
        for value in ('2026-09-02', '2027-01-01', '2026-07-01'):
            meta, obs = truck_packets(); obs['observations'][-1]['date'] = value
            self.assertEqual(m.truck(meta, obs, AT)['status'], 'ambiguous_observations')
    def test_numeric_grammar_and_extremes(self):
        for value in ('1_000', 'NaN', 'Infinity', True, '1e-999', '-1'):
            with self.assertRaises(ValueError): m.number(value)
        self.assertEqual(m.comparison(m.number('1e15'), m.number('1e-300'))['status'], 'outside_numeric_range')
        self.assertEqual(m.number('0e-999'), 0)
    def test_workbook_schema_calendar_labels_and_types_cannot_drift(self):
        original_open = m.xlrd.open_workbook
        for kind in ('sheet', 'date', 'gap', 'label', 'number', 'current', 'column'):
            book = original_open(file_contents=self.workbook, on_demand=True); sheet = book.sheet_by_index(0)
            if kind == 'sheet': book.sheet_names = lambda: ['Unexpected']
            if kind == 'date': sheet._cell_types[2][0] = m.xlrd.XL_CELL_NUMBER
            if kind == 'gap': sheet._cell_values[2][0] = sheet._cell_values[1][0]
            if kind == 'label': sheet._cell_values[2][-1] = 'Changed normalization'
            if kind == 'number': sheet._cell_types[1520][1] = m.xlrd.XL_CELL_BOOLEAN
            if kind == 'column': sheet._cell_values[0][20] = 'REMOVED_OR_RENAMED_SOURCE'
            with patch.object(m.xlrd, 'open_workbook', return_value=book), self.assertRaises(ValueError):
                m.gpr(self.workbook, '2026-08-27T12:20:00Z' if kind == 'current' else AT)
    def test_predecessor_failures_do_not_acquire_or_publish(self):
        for kind in ('absent', 'denied', 'truncated', 'future', 'malformed'):
            mem = Memory()
            if kind == 'absent': del mem.data[store.HEAD]
            if kind == 'denied': mem.denied.add(store.HEAD)
            if kind == 'truncated': mem.truncated.add(store.HEAD)
            if kind == 'future': mem.data[store.HEAD] = store.encode({'generated_at': '2027-01-01T00:00:00Z'})
            if kind == 'malformed': mem.data[store.HEAD] = b'{"zero":0,"zero":1}'
            before = mem.data.get(store.HEAD)
            with self.assertRaises((ValueError, Error)): self.execute(mem)
            self.assertEqual(mem.data.get(store.HEAD), before); self.assertEqual(self.calls, [])
    def test_truncated_redirected_source_never_hidden_by_legacy_catches(self):
        for kind in ('length', 'redirect', 'body'):
            mem = Memory(); prior = mem.data[store.HEAD]
            def source(req, timeout):
                response = self.source(req, timeout)
                if kind == 'length': response.headers['Content-Length'] = '999999'
                if kind == 'redirect': response.url = 'https://evil.invalid/'
                if kind == 'body': response.read = lambda *a: (_ for _ in ()).throw(OSError('partial'))
                return response
            with self.assertRaises(ValueError): self.execute(mem, source)
            self.assertEqual(mem.data[store.HEAD], prior)
    def test_retention_failure_never_hidden_or_published(self):
        mem = Memory(); prior = mem.data[store.HEAD]; original = mem.put_object
        def deny(**kw):
            if kw['Body'] == self.workbook: raise Error('AccessDenied')
            return original(**kw)
        mem.put_object = deny
        with self.assertRaises(ValueError): self.execute(mem)
        self.assertEqual(mem.data[store.HEAD], prior)
    def test_429_not_retried_and_missing_sources_replay_exactly(self):
        def source(req, timeout):
            url = req if isinstance(req, str) else req.full_url
            if 'api.stlouisfed.org' in url:
                self.calls.append(url); return Response(b'Rate limited', url, 429)
            return self.source(req, timeout)
        mem, packet, result = self.execute(opener=source)
        self.assertEqual(len([u for u in self.calls if 'api.stlouisfed.org' in u]), 1)
        self.assertEqual(result['quality'], 'partial_measurements'); self.assertIsNone(packet['heavy_truck_sales']['saar_millions'])
        self.assertEqual(store.replay(self.module, mem, self.module.BUCKET, packet)['provider_attempts'], 31)
    def test_concurrent_writer_kept_and_no_rollback(self):
        mem = Memory(); foreign = b'{"foreign":"keep"}'
        def race(key):
            if key == store.HEAD: mem.data[key] = foreign
        mem.race = race
        with self.assertRaises(Error): self.execute(mem)
        self.assertEqual(mem.data[store.HEAD], foreign)
    def test_transport_failure_and_runtime_budget_states_replay(self):
        def source(req, timeout):
            url = req if isinstance(req, str) else req.full_url
            if url == store.GPR:
                self.calls.append(url); raise urllib.error.URLError('fixture-secret-not-real')
            return self.source(req, timeout)
        mem, packet, _ = self.execute(opener=source)
        self.assertEqual(packet['measurement_review']['gpr_status'], 'source_unavailable')
        self.assertEqual(store.replay(self.module, mem, self.module.BUCKET, packet)['gpr_series'], 0)
        self.assertNotIn(b'fixture-secret-not-real', b''.join(mem.data.values()))
        session = store.Session(Memory(), self.module.BUCKET, AT, opener=self.source)
        session.started -= 101
        with self.assertRaisesRegex(ValueError, 'budget_not_attempted'):
            session.urlopen(store.GPR, timeout=12)
        self.assertEqual(session.http[0]['status'], 'budget_not_attempted')
    def test_next_publication_retains_complete_predecessor_without_recursive_growth(self):
        mem, packet, _ = self.execute(); original = mem.data[store.HEAD]
        mem, updated, _ = self.execute(mem, at='2026-09-28T12:20:00+00:00')
        self.assertIn(original, mem.data.values())
        self.assertNotIn('legacy_calculation', updated['legacy_calculation'])
        self.assertEqual(updated['heavy_truck_sales']['old_nested'], 42)
        self.assertEqual(store.replay(self.module, mem, self.module.BUCKET, updated)['gpr_series'], 112)
    def test_whole_replay_refuses_output_source_and_compiler_tampering(self):
        mem, packet, _ = self.execute()
        for key, value in (('sizing_eligible', True), ('generated_at', '2026-09-28T12:20:00Z')):
            bad = deepcopy(packet); bad[key] = value
            with self.assertRaises(ValueError): store.replay(self.module, mem, self.module.BUCKET, bad)
        bad = deepcopy(packet); bad['measurement_review']['gpr']['series']['GPR']['values'][-2] += 1
        with self.assertRaises(ValueError): store.replay(self.module, mem, self.module.BUCKET, bad)
        plan = store.strict(store.retained(mem, self.module.BUCKET, packet['publication_context']['manifest']))
        key = plan['http_attempts'][0]['original']['key']; mem.data[key] += b' '
        with self.assertRaises(ValueError): store.replay(self.module, mem, self.module.BUCKET, packet)
        with patch.object(store, 'compiler_hashes', return_value={'changed': 'x'}), self.assertRaises(ValueError):
            store.replay(self.module, mem, self.module.BUCKET, packet)
    def test_http_events_and_expired_budget_never_touch_sources(self):
        mem = Memory(); self.module.S3 = mem
        self.assertEqual(store.run(self.module, {'httpMethod': 'GET'})['statusCode'], 409)
        class Context:
            def get_remaining_time_in_millis(self): return 1000
        with self.assertRaises(ValueError): store.run(self.module, context=Context(), at=AT)
        self.assertEqual(mem.reads, []); self.assertEqual(mem.writes, [])
    def test_undeclared_paid_or_credential_bearing_redirects_refused(self):
        for url in ('http://api.stlouisfed.org/x', 'https://evil.invalid/x', store.GPR+'?key=x',
                    'https://financialmodelingprep.com/stable/historical-price-eod/full?apikey=x'):
            with self.assertRaises(ValueError): store.identity(url)
        url = 'https://api.stlouisfed.org/fred/series?'+urllib.parse.urlencode(dict(series_id='HTRUCKSSAAR', file_type='json', api_key='never-store', realtime_start=AT[:10], realtime_end=AT[:10]))
        self.assertNotIn('never-store', store.encode(store.identity(url)).decode())


if __name__ == '__main__':
    unittest.main()
