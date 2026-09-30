"""Complete original-response integrity using invented provider/account fixtures only."""
from copy import deepcopy
from email.message import Message
import base64,hashlib,http.client,io,json,math
from pathlib import Path
import sys,types,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(Path(__file__).resolve().parents[1]/'source')]
import portfolio_risk_model as model
from private_portfolio_test_support import Store,load
from test_risk_model import fixture,NOW,evaluate


class Response:
    def __init__(self,raw,chunk=101,lengths=None,status=200,after_read=None):
        self.raw=raw;self.position=0;self.chunk=chunk;self.status=status;self.closed=False;self.read_sizes=[];self.after_read=after_read
        self.headers=Message()
        for value in lengths or []:self.headers['Content-Length']=value
    def read(self,*_):raise AssertionError('Unbounded/fill-to-size provider read forbidden')
    def read1(self,n):
        self.read_sizes.append(n);part=self.raw[self.position:self.position+min(n,self.chunk)];self.position+=len(part)
        if self.after_read:self.after_read(self)
        return part
    def __enter__(self):return self
    def __exit__(self,*_):self.closed=True


class OriginalResponse(unittest.TestCase):
    def setUp(self):
        self.retained=json.loads((ROOT/'tests/fixtures/pre-portfolio-original-synthetic-bundle.json').read_bytes())
        _,_,self.env=load('portfolio-risk',Store());self.env['POLY_KEY']='invented-provider-token'
        self.env['time']=types.SimpleNamespace(monotonic=lambda:0.0)
        self.packet=fixture()[1]['AAA'];self.raw=base64.b64decode(self.packet['_source_evidence']['raw_body_base64'])
    def fetch(self,response,open_hook=None):
        calls=[]
        def open_(request,timeout):
            calls.append((request,timeout))
            if open_hook:open_hook()
            return response
        with patch('urllib.request.urlopen',side_effect=open_):out=self.env['fetch_polygon_bars']('AAA')
        self.assertEqual(len(calls),1);self.assertEqual(calls[0][1],20);self.assertTrue(response.closed)
        return out
    def rejected(self,response,kind='ValueError',open_hook=None):
        self.assertEqual(self.fetch(response,open_hook),{'error':'SOURCE_REQUEST_FAILED','error_type':kind})
    def test_complete_frozen_valid_output_remains_exact(self):
        import test_snapshot_binding as binding_tests
        inputs=deepcopy(self.retained['bundle']['inputs'])
        binding_tests.SnapshotBinding.assert_preserved_math(self,model.build(**inputs),self.retained['output'])
        self.assertEqual(inputs,self.retained['bundle']['inputs'])
        bundle,out=model.freeze(**inputs);self.assertEqual(model.replay(bundle),out)
        binding_tests.SnapshotBinding.assert_preserved_math(self,out,self.retained['output'])
        with self.assertRaisesRegex(ValueError,'code/schema mismatch'):model.replay(self.retained['bundle'])
    def test_all_three_frozen_defects_reject_before_risk(self):
        variants=json.loads((ROOT/'tests/fixtures/pre-portfolio-original-complete-variants.json').read_bytes())
        for name,row in variants.items():
            with self.subTest(name=name):
                candidate=deepcopy(row['complete_packet'])
                self.assertEqual(model.read_bars(candidate,'AAA',NOW),({},['ORIGINAL_RESPONSE_EVIDENCE_INVALID']))
                snapshot,packets=fixture();packets['AAA']=candidate;out=evaluate(snapshot,packets)
                self.assertIsNone(out['holdings_risk']['var_1d_99_dollars']);self.assertFalse(out['permissions']['sizing_eligible'])
                self.assertEqual(candidate,row['complete_packet'])
    def test_strict_complete_json_rejects_unknown_field_ambiguity(self):
        for raw in [b'{"x":1,"x":2}',b'{"x":1,"\\u0078":2}',b'{"a":[{"b":1,"b":2}]}',b'{"x":NaN}',b'{"x":Infinity}',b'{"x":-Infinity}',b'{"x":1e999}',b'{"x":"\\ud800"}',b'{"\\udfff":1}',b'{"x":"\xff"}',b'{}{}',b'[]',b'\xef\xbb\xbf{}','{}'.encode('utf-16')]:
            with self.subTest(raw=raw):
                with self.assertRaises((ValueError,UnicodeError)):model.source_document(raw)
        for depth in (130,1100):
            with self.assertRaises(ValueError):model.source_document(b'{"x":'+b'['*depth+b'0'+b']'*depth+b'}')
    def test_all_json_values_and_large_integers_are_preserved(self):
        raw=' {"meta":{"unicode":"€日本", "zero":0,"missing":null,"flag":false,"id":9007199254740993},"rows":[0,1.0,-0.0]} \n'.encode()
        value=model.source_document(raw)
        self.assertEqual(value,json.loads(raw));self.assertEqual(value['meta']['id'],9007199254740993)
        self.assertNotEqual(model.source_value_bytes(True),model.source_value_bytes(1))
        self.assertNotEqual(model.source_value_bytes(False),model.source_value_bytes(0))
        self.assertNotEqual(model.source_value_bytes(1),model.source_value_bytes(1.0))
        self.assertEqual(model.source_value_bytes({'a':1,'b':None}),model.source_value_bytes({'b':None,'a':1}))
        for bad in [(1,2),{1:'x'},math.inf,object()]:
            with self.assertRaises(ValueError):model.source_value_bytes(bad)
    def test_invalid_evidence_types_hashes_and_base64_return_unavailable(self):
        for evidence in [False,[],[1],1,'x',None,{}, {'raw_body_base64':True}, {'raw_body_base64':'!'}, {**self.packet['_source_evidence'],'body_sha256':'0'*64}]:
            packet=deepcopy(self.packet);packet['_source_evidence']=evidence
            self.assertEqual(model.read_bars(packet,'AAA',NOW),({},['ORIGINAL_RESPONSE_EVIDENCE_INVALID']))
        packet=deepcopy(self.packet);packet['_source_evidence']['raw_body_base64']='A'*(4*((model.MAX_SOURCE_BYTES+2)//3)+1)
        self.assertEqual(model.read_bars(packet,'AAA',NOW),({},['ORIGINAL_RESPONSE_EVIDENCE_INVALID']))
    def test_raw_byte_identity_keeps_every_unicode_row_and_unknown_value(self):
        document=json.loads(self.raw);document['metadata']={'zero':0,'none':None,'unicode':'€日本'}
        raw=json.dumps(document,indent=2,ensure_ascii=False).encode();response=Response(raw,chunk=3,lengths=[str(len(raw))]);out=self.fetch(response)
        evidence=out.pop('_source_evidence');self.assertEqual(out,document)
        self.assertEqual(base64.b64decode(evidence['raw_body_base64']),raw)
        self.assertEqual(evidence['body_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(response.position,len(raw));self.assertGreater(len(response.read_sizes),2)
        self.assertNotIn(self.env['POLY_KEY'],json.dumps(evidence));self.assertNotIn('apiKey',evidence['request'])
    def test_exact_byte_bound_reads_through_eof_and_oversize_rejects(self):
        raw=self.raw+b' '*(model.MAX_SOURCE_BYTES-len(self.raw));response=Response(raw,chunk=65536)
        out=self.fetch(response);self.assertNotIn('error',out)
        self.assertEqual(base64.b64decode(out['_source_evidence']['raw_body_base64']),raw)
        self.assertEqual(response.read_sizes[-1],1)
        response=Response(raw+b' ',chunk=65536);self.rejected(response)
        self.assertEqual(response.position,model.MAX_SOURCE_BYTES+1)
        with self.assertRaises(ValueError):model.source_document(raw+b' ')
    def test_http_lengths_are_complete_and_unambiguous(self):
        for lengths in [['-1'],['+1'],['1.0'],['1,1'],['\n2'],['2\n'],[''],['２'],['2','2'],[str(model.MAX_SOURCE_BYTES+1)],[str(len(self.raw)+1)],[str(len(self.raw)-1)]]:
            with self.subTest(lengths=lengths):self.rejected(Response(self.raw,lengths=lengths))
        self.assertNotIn('error',self.fetch(Response(self.raw,lengths=[str(len(self.raw))])))
        self.assertNotIn('error',self.fetch(Response(self.raw,lengths=[' \t'+str(len(self.raw))+'\t '])))
    def test_noncomplete_status_and_nonbyte_body_fail_closed(self):
        for status in (204,206,304,401,500):self.rejected(Response(self.raw,status=status))
        self.rejected(Response('{}'))
    def test_deadline_includes_headers_and_each_body_chunk(self):
        clock=[0.0];self.env['time']=types.SimpleNamespace(monotonic=lambda:clock[0])
        response=Response(self.raw)
        self.rejected(response,'TimeoutError',lambda:clock.__setitem__(0,20.0));self.assertEqual(response.read_sizes,[])
        clock[0]=0.0
        response=Response(self.raw,chunk=3,after_read=lambda _:clock.__setitem__(0,clock[0]+5))
        self.rejected(response,'TimeoutError');self.assertEqual(len(response.read_sizes),4)
        self.assertLess(response.position,len(self.raw))
    def test_partial_read_failure_never_publishes_a_prefix(self):
        def fail(response):
            if response.position>100:raise OSError('invented-provider-token must never escape')
        self.rejected(Response(self.raw,chunk=60,after_read=fail),'OSError')
    def test_decoding_and_reserved_evidence_errors_never_leak_provider_data(self):
        for raw in [b'{"x":1,"x":2}',b'{"_source_evidence":{"x":1}}',b'{"echo":"invented-provider-token"}']:
            self.rejected(Response(raw))
    def test_no_credential_means_no_request(self):
        self.env['POLY_KEY']=''
        with patch('urllib.request.urlopen',side_effect=AssertionError('No request without credential')):
            self.assertEqual(self.env['fetch_polygon_bars']('AAA'),{'error':'PROVIDER_CREDENTIAL_UNAVAILABLE'})
    def test_real_stdlib_http_content_length_and_chunked_bodies(self):
        class Socket:
            def __init__(self,raw):self.raw=raw
            def makefile(self,*_,**kw):return io.BytesIO(self.raw)
        pieces=[self.raw[i:i+17] for i in range(0,len(self.raw),17)]
        messages=[b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(self.raw)).encode()+b'\r\n\r\n'+self.raw,
            b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n'+b''.join(format(len(p),'x').encode()+b'\r\n'+p+b'\r\n' for p in pieces)+b'0\r\n\r\n']
        for message in messages:
            response=http.client.HTTPResponse(Socket(message));response.begin();out=self.fetch(response)
            self.assertEqual(base64.b64decode(out['_source_evidence']['raw_body_base64']),self.raw)
    def test_real_stdlib_http_truncation_is_unavailable(self):
        class Socket:
            def __init__(self,raw):self.raw=raw
            def makefile(self,*_,**kw):return io.BytesIO(self.raw)
        for message in [b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(self.raw)+1).encode()+b'\r\n\r\n'+self.raw,
                b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n20\r\n{}']:
            response=http.client.HTTPResponse(Socket(message));response.begin();out=self.fetch(response)
            self.assertEqual(out['error'],'SOURCE_REQUEST_FAILED');self.assertEqual(set(out),{'error','error_type'})
    def test_all_other_original_source_math_and_snapshot_functions_remain_identical(self):
        import ast
        prior=ast.parse((ROOT/'tests/fixtures/pre-risk-calculation/stage448-predecessor-portfolio_risk_model.py.txt').read_text(encoding='utf-8'))
        current=ast.parse((ROOT/'aws/lambdas/justhodl-portfolio-risk/source/portfolio_risk_model.py').read_text(encoding='utf-8'))
        before={n.name:ast.dump(n,include_attributes=False) for n in prior.body if isinstance(n,ast.FunctionDef)}
        after={n.name:ast.dump(n,include_attributes=False) for n in current.body if isinstance(n,ast.FunctionDef)}
        self.assertEqual({name for name,code in before.items() if after[name]!=code},{'build','exposure_view','code_identity'})
        self.assertEqual(set(after)-set(before),{'cash_equity_identity','risk_capital_book'})


if __name__=='__main__':unittest.main()
