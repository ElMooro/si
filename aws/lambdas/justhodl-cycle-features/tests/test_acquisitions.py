"""Whole original inputs, native compiler replay and publication failure cases."""
from copy import deepcopy
from io import BytesIO, StringIO
from unittest.mock import patch
import csv
import gzip
import unittest
import urllib.error
from run_tests import load, fixture, Memory, Error, NOW, pub, sources


class Response(BytesIO):
    def __init__(self, raw, status=200, headers=None):
        super().__init__(raw)
        self.status = status
        self.headers = {'Content-Length': str(len(raw)), **(headers or {})}


def series_body(env):
    out = StringIO();writer = csv.writer(out, lineterminator='\n')
    writer.writerow(('FREQ', 'REF_AREA', 'MEASURE', 'ACTIVITY', 'ADJUSTMENT', 'TRANSFORMATION', 'UNIT_MEASURE', 'TIME_PERIOD', 'OBS_VALUE'))
    for iso in env['ISO3']:
        for month in env['month_grid']('2023-01', '2026-08'):
            for measure, value in (('IRLT', 4), ('IR3TIB', 3), ('LI', 100)):
                writer.writerow(('M', iso, measure, '_Z', 'Y', '_Z', 'PC', month, value))
    return out.getvalue().encode('utf-8')


def run_fixture():
    store, _ = fixture();env = load(store);body = series_body(env)
    store.seed('data/global-sovereign.json', pub.encode({'generated_at': '2026-09-27T06:15:00Z',
               'countries': [{'country': 'United States', 'yield_10y_pct': 99, 'cb_rate_pct': 0}]}))
    store.seed('data/asia-leads.json', b'{}')
    responses = []
    def opener(*args, **kw):
        response = Response(body);responses.append(response);return response
    with patch.object(sources.urllib.request, 'build_opener') as build:
        build.return_value.open = opener
        env['lambda_handler']()
    assert all(r.closed for r in responses)
    doc = pub.strict(pub.decoded(store.rows[pub.HEAD]));meta = pub.strict(store.rows[pub.MANIFEST])
    return store, env, doc, meta


class Acquisitions(unittest.TestCase):
    def test_native_whole_34_country_compiler_replays_every_value_and_permission(self):
        store, env, doc, meta = run_fixture()
        self.assertIsNone(env['AUDIT'])
        before = deepcopy(store.rows)
        result = sources.replay_publication(env, doc, meta, store.rows.__getitem__)
        self.assertEqual(result['countries'], 34)
        self.assertEqual(result['feature_series'], 68)
        self.assertEqual(result['observations'], 34*44*2)
        self.assertFalse(result['model_qualified'])
        self.assertEqual(before, store.rows)
        self.assertTrue(all(c['features']['curve']['latest_value'] == 1 for c in doc['countries'].values()))
        self.assertFalse(doc['source_evidence']['original_source_replay_verified'])

    def test_changes_to_originals_projection_manifest_counts_and_compiler_are_rejected(self):
        store, env, doc, meta = run_fixture()
        for mode in ('body', 'country', 'count', 'permission', 'compiler', 'clock', 'extra_event'):
            d, m, rows = deepcopy(doc), deepcopy(meta), deepcopy(store.rows)
            ref = d['source_evidence']['manifest'];capture = pub.strict(rows[ref['key']])
            if mode == 'body':rows[capture['events'][0]['attempts'][0]['original']['key']] += b'x'
            if mode == 'country':d['countries']['USA']['features']['curve']['values'][270] = 99
            if mode == 'count':d['source_evidence']['http_attempts'] += 1;m['source_evidence'] = d['source_evidence']
            if mode == 'permission':d['sizing_eligible'] = True
            if mode in ('compiler', 'clock', 'extra_event'):
                if mode == 'compiler':capture['compiler_sha256']['cycle_sources.py'] = '0'*64
                if mode == 'clock':capture['clocks'][0] = '2000-01-01T00:00:00Z'
                if mode == 'extra_event':capture['events'].append(deepcopy(capture['events'][-1]))
                raw=pub.encode(capture);new={'key':pub.PRIVATE+pub.sha(raw)+'.bin','sha256':pub.sha(raw),'bytes':len(raw)}
                rows[new['key']]=raw;d['source_evidence']['manifest']=new;m['source_evidence']=d['source_evidence']
            with self.subTest(mode=mode), self.assertRaises((ValueError, sources.EvidenceError)):
                sources.replay_publication(env, d, m, rows.__getitem__)

    def test_stored_gzip_input_has_complete_raw_identity_and_decoded_identity(self):
        store=Memory();load(store);raw=gzip.compress(b'csv entire\n'*5000)
        key='data/warm/oecd/cycle/DF_CLI.csv.gz';store.seed(key,raw)
        cap=sources.Capture(store,'b',NOW.isoformat(),[])
        body,lm=cap.get_bytes(key);event=cap.events[0]
        self.assertEqual(event['original']['sha256'],pub.sha(raw))
        self.assertEqual(store.rows[event['original']['key']],raw)
        self.assertEqual(event['decoded_bytes'],len(body))
        self.assertEqual(event['decoded_sha256'],pub.sha(body))
        self.assertNotEqual(event['original']['sha256'],event['decoded_sha256'])

    def test_failed_http_bodies_and_original_retry_policy_are_preserved(self):
        store=Memory();load(store);calls=[];sleeps=[]
        def open_request(request,timeout):
            calls.append(request.full_url)
            if len(calls)==1:
                raise urllib.error.HTTPError(request.full_url,429,'quota',{'Content-Length':'5'},BytesIO(b'quota'))
            return Response(gzip.compress(b'complete'),headers={'Content-Encoding':'gzip'})
        cap=sources.Capture(store,'b',NOW.isoformat(),['https://sdmx.oecd.org/test'],open_request,sleeps.append)
        self.assertEqual(cap.http_get('https://sdmx.oecd.org/test'),(b'complete',200))
        self.assertEqual(sleeps,[15]);self.assertEqual(len(calls),2)
        event=cap.events[0]
        self.assertEqual(store.rows[event['attempts'][0]['original']['key']],b'quota')
        self.assertEqual(event['selected_attempt'],1)
        capture={'contract':'cycle-source-acquisition.v1','compiler_sha256':sources.compiler_hashes(),'events':[event]}
        replay=sources.Replay(capture,store.rows.__getitem__,calls)
        self.assertEqual(replay.http_get(calls[0]),(b'complete',200))

    def test_oversize_partial_length_encoding_and_truncated_gzip_never_become_complete(self):
        for mode in ('bound','length','gzip','encoding','bad_declared_gzip'):
            store=Memory();load(store);responses=[]
            def open_request(*args,**kw):
                raw=b'x'*1025 if mode=='bound' else gzip.compress(b'whole')
                headers={}
                if mode=='length':headers['Content-Length']='999'
                if mode=='gzip':raw=raw[:-2]
                if mode=='encoding':headers['Content-Encoding']='br'
                if mode=='bad_declared_gzip':raw=b'not gzip';headers['Content-Encoding']='gzip'
                r=Response(raw,headers=headers);responses.append(r);return r
            cap=sources.Capture(store,'b',NOW.isoformat(),['https://stats.bis.org/test'],open_request,lambda n:None)
            with patch.object(pub,'LIMIT',1024),self.subTest(mode=mode):
                result,status=cap.http_get('https://stats.bis.org/test')
                self.assertIsNone(result);self.assertEqual(cap.events[0]['status'],'unavailable')
            self.assertTrue(all(r.closed for r in responses))
            for attempt in cap.events[0]['attempts']:
                self.assertNotEqual(attempt.get('status'),'complete')
                if mode in ('bound','length'):self.assertNotIn('original',attempt)

    def test_retention_failure_aborts_before_any_cache_or_public_output_and_resets_session(self):
        store,_=fixture();env=load(store);body=series_body(env)
        original_put=store.put_object
        def fail_input_archive(**kw):
            if kw['Key']==pub.PRIVATE+pub.sha(body)+'.bin':raise Error('AccessDenied')
            return original_put(**kw)
        store.put_object=fail_input_archive
        with patch.object(sources.urllib.request,'build_opener') as build:
            build.return_value.open=lambda *a,**kw:Response(body)
            with self.assertRaises(sources.EvidenceError):env['lambda_handler']()
        self.assertIsNone(env['AUDIT'])
        self.assertFalse(any(key.startswith('data/') for key in store.writes))

    def test_capture_failure_propagates_from_every_optional_source_loader(self):
        env=load(Memory())
        def fail(*args,**kw):raise sources.EvidenceError('retention unavailable')
        env['get_bytes']=fail;env['http_get']=lambda *args,**kw:(None,'HTTP 503')
        for name in ('load_oecd_cli','load_oecd_kei','load_oecd_unemployment','load_bis','load_eurostat','load_fleet_feeds'):
            with self.subTest(loader=name),self.assertRaises(sources.EvidenceError):
                env[name]({}, {})

    def test_private_source_redirect_and_undeclared_request_are_refused_before_read(self):
        store=Memory();load(store);cap=sources.Capture(store,'b',NOW.isoformat(),[])
        with self.assertRaises(sources.EvidenceError):cap.get_bytes('data/portfolio.json')
        with self.assertRaises(sources.EvidenceError):cap.http_get('https://not-a-source.example/')
        self.assertEqual(cap.events,[])
        self.assertIsNone(sources.NoRedirect().redirect_request(None,None,302,'',{},'https://other.example/'))


if __name__=='__main__':
    unittest.main(verbosity=2)
