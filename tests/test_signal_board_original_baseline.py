"""The composite baseline retains the entire registry without executing it."""
from pathlib import Path
import hashlib,io,json,sys,unittest,urllib.error,zipfile
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops/checks','aws/shared')]
import ops_6141_signal_board_original_baseline as baseline

class Storage:
    def __init__(self):self.objects={};self.puts=[]
    def put_object(self,**kw):self.objects[kw['Key']]=kw['Body'];self.puts.append(kw)
    def get_object(self,**kw):return {'Body':io.BytesIO(self.objects[kw['Key']])}

class Tests(unittest.TestCase):
    def test_all_literal_registry_rows_and_duplicate_roots_remain_visible(self):
        text=(ROOT/'aws/lambdas/justhodl-signal-board/source/lambda_function.py').read_text(encoding='utf-8')
        rows=baseline.feeds(text)
        self.assertGreater(len(rows),90)
        self.assertEqual(len({r['engine'] for r in rows}),len(rows))
        shared=[r for r in rows if r['source_key']=='data/liquidity-inflection.json']
        self.assertEqual(len(shared),2)
        excluded={r['source_key'] for r in rows if r['capture_scope']=='excluded_private_account_input'}
        self.assertEqual(excluded,{'data/pm-decision.json','data/sizing.json'})
        self.assertTrue(all(r['normalizer'].startswith('n_') for r in rows))

    def test_registry_is_not_executed_and_ambiguous_or_dynamic_rows_are_rejected(self):
        text="raise RuntimeError('must never execute')\nFEEDS=[('A','macro','data/a.json',n_a)]"
        self.assertEqual(baseline.feeds(text)[0]['engine'],'A')
        for bad in ("FEEDS=make_feeds()", "FEEDS=[('A','m','../secret',n_a)]", "FEEDS=[('A','m','data/a.json',n_a()),]", "FEEDS=[('A','m','data/a.json',n_a),('A','m','data/b.json',n_b)]"):
            with self.assertRaises(ValueError):baseline.feeds(bad)

    def test_page_registry_keeps_every_native_row_and_original_page_byte(self):
        source=(ROOT/'aws/lambdas/justhodl-signal-board/source/lambda_function.py').read_text(encoding='utf-8')
        registry=json.loads((ROOT/'assets/signal-board-registry.json').read_bytes())
        self.assertEqual(registry['feeds'],baseline.feeds(source))
        self.assertEqual(registry['baseline_manifest'],'746cfc5ea9abc8dc933c7c1bbefc36c1adebe5db00d78c2f38cfe97031ef35d6')
        original=(ROOT/'tests/fixtures/legacy-signal-board-stage140.html.txt').read_bytes()
        self.assertEqual(len(original),20970)
        self.assertEqual(hashlib.sha256(original).hexdigest(),'2953393839b59c6a31e39e87f4bff56572f8ede48a9198dac8fbebd311a29e77')

    def test_private_or_unreviewed_keys_are_blocked_before_transport(self):
        called=[]
        for key in ('data/pm-decision.json','data/sizing.json','data/portfolio/snapshot.json','https://example.invalid','data/../a.json'):
            with self.assertRaises(ValueError):baseline.public_response(key,lambda *a,**kw:called.append(True))
        self.assertEqual(called,[])

    def test_public_reads_are_bounded_readonly_and_retain_whole_http_errors(self):
        raw=b'complete denied response';requests=[]
        def transport(req,timeout):
            requests.append(req);self.assertEqual(timeout,30)
            raise urllib.error.HTTPError(req.full_url,403,'denied',{'Content-Type':'text/plain','Set-Cookie':'private'},io.BytesIO(raw))
        body,status,headers=baseline.public_response('data/example.json',transport)
        self.assertEqual(body,raw);self.assertEqual(status,403);self.assertEqual(len(requests),1)
        self.assertTrue(requests[0].full_url.endswith('?exact=1&nogen=1'))
        self.assertNotIn('set-cookie',headers);self.assertEqual(requests[0].get_method(),'GET')

    def test_whole_bodies_zero_and_empty_files_survive_without_authority(self):
        client=Storage();body=b'whole\x00'+b'x'*65000
        ref=baseline.retain(client,body);self.assertEqual(client.objects[ref['key']],body)
        self.assertEqual(ref['sha256'],hashlib.sha256(body).hexdigest());self.assertEqual(client.puts[0]['IfNoneMatch'],'*')
        self.assertEqual(baseline.retain(client,b'',allow_empty=True)['bytes'],0)
        out=baseline.summarize(json.dumps({'engines':[{'signal':0},{'signal':None},{'signal':True}],'calls_eligible':True}).encode())
        self.assertEqual(out['numeric_signal_rows'],1);self.assertEqual(out['missing_signal_rows'],1)
        self.assertFalse(out['forecast_qualified']);self.assertTrue(out['permission_declarations']['calls_eligible'])
        self.assertEqual(baseline.summarize(b'bad json')['status'],'whole_unparsed_response')

    def test_complete_zip_keeps_duplicate_names_and_drift_as_evidence(self):
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as z:
            z.writestr('lambda_function.py',b'first');z.writestr('lambda_function.py',b'second')
        result=baseline.package_inventory(stream.getvalue(),{'lambda_function.py':b'first'})
        self.assertEqual(len(result['complete_zip_members']),2);self.assertFalse(result['code_matches_repository'])
        self.assertFalse(result['release_verification'])

    def test_baseline_has_no_producer_or_account_actions(self):
        text=Path(baseline.__file__).read_text(encoding='utf-8')
        for forbidden in ('.invoke(','.update_function_code(','.put_rule(','.update_schedule(','.get_parameter(','.get_secret_value('):self.assertNotIn(forbidden,text)
        for required in ('sys.exit(1)','journal(s3,progress,True)',"assert privacy['all_denied']",'scheduled_qualified_packages','private_account_reads=0'):
            self.assertIn(required,text)

if __name__=='__main__':unittest.main(verbosity=2)
