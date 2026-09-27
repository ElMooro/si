"""Actual entrypoint, complete acquisition, replay and publication boundaries."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import BytesIO
from copy import deepcopy
from unittest.mock import patch
import importlib.util
import ast
import json
import sys
import types
import unittest
import urllib.error
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(HERE.parent/'source'))
import recession_research as store
NOW = datetime(2026, 9, 27, 12, 40, 1, 4321, tzinfo=timezone.utc)


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self):
        self.rows, self.versions, self.writes, self.reads = {}, {}, [], []
        self.fail = self.after = None
    def seed(self, key, raw):
        self.rows[key] = raw
        self.versions[key] = self.versions.get(key, 0)+1
    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        if self.fail == Key:
            raise Error('AccessDenied')
        if Key not in self.rows:
            raise Error('NoSuchKey')
        raw = self.rows[Key]
        return {'Body': BytesIO(raw), 'ContentLength': len(raw), 'ETag': str(self.versions[Key]), 'LastModified': NOW-timedelta(days=1)}
    def put_object(self, Bucket, Key, Body, **kw):
        if self.fail == 'retention' and Key.startswith(store.PRIVATE):
            raise Error('AccessDenied')
        if kw.get('IfNoneMatch') == '*' and Key in self.rows:
            raise Error('PreconditionFailed')
        if kw.get('IfMatch') is not None and kw['IfMatch'] != str(self.versions.get(Key)):
            raise Error('PreconditionFailed')
        self.seed(Key, Body)
        self.writes.append(Key)
        if self.after:
            self.after(Key)


class Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


def fixture():
    memory = Memory()
    memory.seed(store.HEAD, store.encode({'generated_at': (NOW-timedelta(days=1)).isoformat(), 'old_rows': list(range(1200))}))
    source = (ROOT/'aws/lambdas/justhodl-global-business-cycle/source/lambda_function.py').read_text(encoding='utf-8')
    mapping = next(n.value for n in ast.parse(source).body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'COUNTRY_MAP' for t in n.targets))
    countries = {iso: {'country_name': name, 'region': region, 'gdp_weight': 1.0,
                       'phase': 'EXPANSION', 'cli_level': 100.0, 'six_month_change': 0.0,
                       'dist_200ma_pct': 0.0, 'z_5y': 0.0, 'latest_date': '2026-09-25'}
                 for iso, symbol, region, name, weight, iso2 in ast.literal_eval(mapping)}
    packets = ({'by_country': countries}, {'period': '2026-08', 'by_country': {}}, {'ports': []},
               {'credit_impulse': {'value': 0.0}}, {'indicators': {'USAGDPYY': {'v': 0.0}, 'CHNIPYY': {'v': -1.0}}})
    for key, packet in zip(store.INPUTS, packets):
        memory.seed(key, store.encode(packet))
    return memory


def module(memory):
    spec = importlib.util.spec_from_file_location('recession_under_test', HERE.parent/'source/lambda_function.py')
    loaded = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **kw: memory)}):
        spec.loader.exec_module(loaded)
    loaded.datetime = Frozen
    loaded.FRED_KEY = 'test-credential-never-retained'
    return loaded


def fred(request, timeout):
    series = store.urllib.parse.parse_qs(store.urllib.parse.urlsplit(request.full_url).query)['series_id'][0]
    return BytesIO(store.encode({'observations': [{'date': '2026-09-25', 'value': '0.0' if series == 'T10Y3M' else '-0.07'}],
                                'complete_response_tail': list(range(2500))}))


def run(memory, loaded=None, opener=fred):
    loaded = loaded or module(memory)
    session = store.Session
    with patch.object(store, 'Session', side_effect=lambda *a, **k: session(*a, **k, now=lambda: NOW, opener=opener)):
        result = loaded.lambda_handler(None, None)
    public = store.strict(memory.rows[store.HEAD])
    manifest = store.strict(memory.rows[public['publication_context']['acquisition_manifest']['key']])
    return loaded, result, public, manifest


class Tests(unittest.TestCase):
    def test_all_inherited_calculation_functions_are_preserved_except_reviewed_io_and_entrypoint(self):
        old = ast.parse((ROOT/'tests/fixtures/pre-research-global-recession.py.txt').read_text(encoding='utf-8'))
        new = ast.parse((HERE.parent/'source/lambda_function.py').read_text(encoding='utf-8'))
        funcs = {n.name: n for n in new.body if isinstance(n, ast.FunctionDef)}
        for node in old.body:
            if isinstance(node, ast.FunctionDef) and node.name not in ('lambda_handler', 'read_feed', 'fred_latest'):
                self.assertEqual(ast.dump(node), ast.dump(funcs[node.name]))
        old_core = next(n for n in old.body if isinstance(n, ast.FunctionDef) and n.name == 'lambda_handler')
        new_core = next(n for n in new.body if isinstance(n, ast.FunctionDef) and n.name == 'lambda_handler')
        self.assertEqual(ast.dump(old_core), ast.dump(new_core))

    def test_actual_entrypoint_retains_every_input_and_both_stages_and_only_publishes_once(self):
        memory = fixture(); old = memory.rows[store.HEAD]
        loaded, result, packet, manifest = run(memory)
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(memory.writes.count(store.HEAD), 1)
        self.assertEqual(memory.rows[manifest['predecessor']['key']], old)
        self.assertEqual(len(manifest['operations']), 7)
        self.assertEqual(set(x['path'] for x in manifest['operations'] if x['kind']=='derived_s3'), set(store.INPUTS))
        for op in manifest['operations']:
            self.assertEqual(store.sha(memory.rows[op['original']['key']]), op['original']['sha256'])
        self.assertNotIn('test-credential-never-retained', str(manifest))
        self.assertEqual(set(manifest['compiler_sha256']), store.COMPILERS)
        self.assertEqual(len(packet['countries']), 34)
        self.assertEqual(packet['us_crosscheck']['yield_curve_probit']['t10y3m_spread_pp'], 0.0)
        self.assertIsNone(packet['global_recession_prob_pct'])
        self.assertIs(loaded.s3, memory); self.assertIsNone(loaded._research_session)
        for key in store.PERMISSIONS:
            self.assertIs(packet[key], False)
        raw_final = store.strict(memory.rows[manifest['complete_native_stages'][-1]['key']])
        self.assertEqual(packet['unqualified_legacy_calculation'], raw_final)

    def test_offline_replay_reproduces_both_full_calculations_and_restores_state(self):
        memory = fixture(); loaded, _, packet, manifest = run(memory)
        writes = list(memory.writes)
        result = store.replay_native(loaded, manifest, lambda k: memory.rows[k])
        self.assertEqual(result['complete_native_stages'], 2)
        self.assertEqual(result['final_calculation_sha256'], manifest['complete_native_stages'][-1]['sha256'])
        self.assertEqual(memory.writes, writes); self.assertIs(loaded.s3, memory)
        for mutate in (lambda m: m['processing_clocks'].append(NOW.isoformat()),
                       lambda m: m['operations'].reverse(),
                       lambda m: m['compiler_sha256'].update({'extra.py':'a'*64}),
                       lambda m: m['operations'][0]['original'].update(key='private/account.json'),
                       lambda m: m['complete_native_stages'][1].update(sha256='a'*64)):
            bad = deepcopy(manifest); mutate(bad)
            with self.assertRaises(store.EvidenceError):
                store.replay_native(loaded, bad, lambda k: memory.rows[k])
            self.assertIs(loaded.s3, memory)

    def test_late_bus_failure_never_publishes_first_stage(self):
        memory = fixture(); old = memory.rows[store.HEAD]
        memory.seed(store.INPUTS[-1], store.encode({'indicators': {'badGDPYY': None}}))
        loaded = module(memory)
        with self.assertRaises(store.EvidenceError):
            run(memory, loaded)
        self.assertEqual(memory.rows[store.HEAD], old); self.assertNotIn(store.HEAD, memory.writes)
        self.assertIs(loaded.s3, memory); self.assertIs(loaded.datetime, Frozen)

    def test_missing_optional_context_remains_missing_but_denied_or_corrupt_input_aborts(self):
        memory = fixture(); del memory.rows[store.INPUTS[2]]
        _, _, _, manifest = run(memory)
        self.assertEqual(next(x for x in manifest['operations'] if x.get('path')==store.INPUTS[2])['status'], 'missing')
        for raw in (b'{"x":0,"x":1}', b'{"x":1e999}', b'{"x":NaN}', b'[]', b'\xff'):
            memory = fixture(); memory.seed(store.INPUTS[2], raw); old = memory.rows[store.HEAD]
            with self.assertRaises(Exception): run(memory)
            self.assertEqual(memory.rows[store.HEAD], old)
        memory = fixture(); memory.fail = store.INPUTS[2]
        with self.assertRaises(store.EvidenceError): run(memory)
        self.assertNotIn(store.HEAD, memory.writes)

    def test_predecessor_must_be_whole_versioned_retained_and_older(self):
        for mode in ('future', 'denied', 'retention', 'truncated', 'unversioned'):
            memory=fixture(); get=memory.get_object
            if mode=='future': memory.seed(store.HEAD,store.encode({'generated_at':(NOW+timedelta(seconds=1)).isoformat()}))
            elif mode=='denied': memory.fail=store.HEAD
            elif mode=='retention': memory.fail='retention'
            elif mode=='truncated': memory.get_object=lambda **k:{**get(**k),'ContentLength':123}
            else: memory.get_object=lambda **k:{**get(**k),'ETag':None}
            with self.assertRaises(Exception): run(memory)
            self.assertNotIn(store.HEAD,memory.writes)

    def test_competing_head_is_never_overwritten_or_rolled_back(self):
        memory=fixture(); competing=store.encode({'generated_at':NOW.isoformat(),'owner':'other writer'})
        def after(key):
            if key.startswith(store.PRIVATE) and b'recession-publication-attempt.v1' in memory.rows[key]:
                memory.seed(store.HEAD,competing)
        memory.after=after
        with self.assertRaisesRegex(store.EvidenceError,'Head changed'):run(memory)
        self.assertEqual(memory.rows[store.HEAD],competing);self.assertNotIn(store.HEAD,memory.writes)

    def test_http_error_body_is_retained_without_credentials_and_replays_as_missing_measurement(self):
        def unavailable(request,timeout):
            raise urllib.error.HTTPError(request.full_url,429,'limit',{},BytesIO(b'complete provider failure'))
        memory=fixture(); loaded,_,packet,manifest=run(memory,opener=unavailable)
        http=[r for r in manifest['operations'] if r['kind']=='fred_http']
        self.assertEqual(len(http),2);self.assertTrue(all(r['http_status']==429 for r in http))
        self.assertTrue(all(memory.rows[r['original']['key']]==b'complete provider failure' for r in http))
        self.assertNotIn('test-credential-never-retained',str(manifest))
        self.assertNotIn('yield_curve_probit',packet['us_crosscheck'])
        store.replay_native(loaded,manifest,lambda k:memory.rows[k])

    def test_invalid_provider_data_retained_before_failure_and_no_output_is_fabricated(self):
        for raw in (b'{"observations":[],"observations":[]}', b'{"observations":[{"date":"2026-09-25","value":"NaN"}]}',
                    b'{"observations":[{"date":"2099-01-01","value":"0"}]}', b'\xff', b'{}',
                    b'{"observations":[{"date":"2026-02-31","value":"0"}]}'):
            memory=fixture();old=memory.rows[store.HEAD]
            with self.assertRaises(Exception):run(memory,opener=lambda *a,**k:BytesIO(raw))
            self.assertIn(store.PRIVATE+store.sha(raw)+'.bin',memory.rows)
            self.assertEqual(memory.rows[store.HEAD],old)

    def test_incomplete_http_and_retention_readback_failure_abort_instead_of_becoming_missing(self):
        memory=fixture();old=memory.rows[store.HEAD]
        def short(*a,**k):
            body=BytesIO(b'{"observations":[]}');body.headers={'Content-Length':'999'};return body
        with self.assertRaises(store.EvidenceError):run(memory,opener=short)
        self.assertEqual(memory.rows[store.HEAD],old)
        memory=fixture();get=memory.get_object
        def denied(**kw):
            if kw['Key'].startswith(store.PRIVATE) and kw['Key'] in memory.rows and b'complete_response_tail' in memory.rows[kw['Key']]:
                raise Error('AccessDenied')
            return get(**kw)
        memory.get_object=denied
        with self.assertRaises(store.EvidenceError):run(memory)
        self.assertNotIn(store.HEAD,memory.writes)

    def test_unreviewed_paths_and_provider_queries_never_read_or_request(self):
        memory=fixture();session=store.Session(memory,'b',NOW.isoformat(),now=lambda:NOW,opener=lambda *a,**k:self.fail('network'))
        before=list(memory.reads)
        with self.assertRaises(store.EvidenceError):session.feed('data/portfolio.json')
        with self.assertRaises(store.EvidenceError):session.fred(store.urllib.request.Request('https://example.com/private'),20)
        self.assertEqual(memory.reads,before)
        body=BytesIO(b'abcdef')
        with patch.object(store,'LIMIT',3),self.assertRaises(store.EvidenceError):store.bounded(body)
        self.assertTrue(body.closed)

    def test_projection_preserves_complete_legacy_details_but_removes_active_authority(self):
        memory=fixture();_,_,packet,manifest=run(memory)
        old=packet['unqualified_legacy_calculation'];projected=store.projection(old)
        self.assertEqual(projected['unqualified_legacy_calculation'],old)
        self.assertIsNone(projected['us_crosscheck']['yield_curve_probit']['prob_12m_pct'])
        self.assertTrue(all(r['recession_prob_pct'] is None and r['contribution_pp'] is None for r in projected['countries']))
        self.assertTrue(all(r['recession_prob_pct'] is None for r in projected['by_region'].values()))
        self.assertTrue(projected['equity_beta_guidance']['rule'].startswith('WAIT'))
        self.assertEqual(projected['countries'][0]['six_month_change'],0.0)
        self.assertIn('* 40',projected['methodology']['actual_modifiers']['momentum_6m'])


if __name__=='__main__':unittest.main(verbosity=2)
