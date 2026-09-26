"""Prove the ICI baseline cannot turn missing data into a production packet."""
from pathlib import Path
import io,json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops/checks','aws/shared')]
import ops_6165_ici_original_baseline as baseline

class Storage:
    def __init__(self):self.objects={};self.puts=[]
    def put_object(self,**kw):
        if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.objects:
            exc=RuntimeError('already claimed');exc.response={'Error':{'Code':'PreconditionFailed'}};raise exc
        self.objects[kw['Key']]=kw['Body'];self.puts.append(kw)
    def get_object(self,**kw):return {'Body':io.BytesIO(self.objects[kw['Key']])}

class Tests(unittest.TestCase):
    def test_source_allowlist_and_whole_http_errors(self):
        calls=[];body=b'complete source denial'
        def transport(req,timeout):
            calls.append(req);self.assertEqual(timeout,30)
            raise urllib.error.HTTPError(req.full_url,403,'denied',{'Content-Length':str(len(body)),'Set-Cookie':'private'},io.BytesIO(body))
        raw,status,headers=baseline.public_response('mmf',transport)
        self.assertEqual(raw,body);self.assertEqual(status,403);self.assertNotIn('set-cookie',headers)
        self.assertEqual(len(calls),1);self.assertEqual(calls[0].get_method(),'GET')
        self.assertFalse({'authorization','cookie'}&{k.lower() for k in calls[0].headers})
        for name in ('https://evil.invalid','unknown','../private'):
            with self.assertRaises(ValueError):baseline.public_response(name,transport)
        self.assertEqual(len(calls),1)

    def test_partial_source_and_oversize_are_rejected(self):
        stream=io.BytesIO(b'a');stream.status=200;stream.headers={'Content-Length':'2'}
        with self.assertRaises(ValueError):baseline.public_response('combined_flows',lambda *a,**kw:stream)
        with self.assertRaises(ValueError):baseline.bounded(io.BytesIO(b'abc'),2)

    def test_all_baseline_bytes_remain_private_and_content_bound(self):
        client=Storage()
        for raw in (b'',b'whole\x00'+b'x'*65000):
            ref=baseline.retain(client,raw);self.assertEqual(client.objects[ref['key']],raw)
            self.assertEqual(ref['bytes'],len(raw));self.assertEqual(ref['sha256'],baseline.sha(raw))
        self.assertTrue(all(p['Key'].startswith(baseline.PRIVATE) and p['IfNoneMatch']=='*' for p in client.puts))
        baseline.journal(client,{'status':'claimed'},True);self.assertEqual(client.puts[-1]['IfNoneMatch'],'*')

    def test_repeat_claim_and_conflicting_retained_bytes_fail(self):
        client=Storage();baseline.journal(client,{'status':'claimed'},True)
        with self.assertRaises(RuntimeError):baseline.journal(client,{'status':'claimed'},True)
        ref=baseline.retain(client,b'whole');self.assertEqual(baseline.retain(client,b'whole'),ref)
        client.objects[ref['key']]=b'tampered'
        with self.assertRaisesRegex(ValueError,'Retained baseline differs'):baseline.retain(client,b'whole')

    def test_legacy_shapes_do_not_gain_source_qualification(self):
        for raw in (b'bad',b'{}',b'[]',b'null',b'{"signal":2,"calls_eligible":true}'):
            self.assertIs(baseline.summary(raw)['source_qualified'],False)

    def test_no_producer_or_public_history_write_is_present(self):
        text=Path(baseline.__file__).read_text(encoding='utf-8')
        for forbidden in ('.invoke(','.update_function_', '.put_rule(','.update_schedule(','.send_message(','.publish('):
            self.assertNotIn(forbidden,text)
        self.assertEqual(len(baseline.SOURCES),2)
        self.assertEqual(baseline.KEYS,('data/ici-flows.json','data/history/ici-mmf.json','data/history/ici-flows.json'))

if __name__=='__main__':unittest.main()
