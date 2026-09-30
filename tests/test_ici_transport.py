"""Whole invented ICI transport regressions; no real AWS/provider access."""
from pathlib import Path
from io import BytesIO
from email.message import Message
from http.client import HTTPResponse,IncompleteRead
from types import ModuleType
from unittest.mock import patch
import hashlib,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-ici-flows/source'))
import ici_store as store
from test_ici_native import Memory,StorageError,acquire,inputs
from test_ici_research_candidate import GENERATED

class Fragmented(BytesIO):
    def __init__(self,raw,fragment=7):super().__init__(raw);self.fragment=fragment;self.calls=0
    def read(self,n=-1):self.calls+=1;return super().read(min(n,self.fragment))

class Response(Fragmented):
    status=200
    def __init__(self,raw,headers=(),status=200):
        super().__init__(raw);self.headers=Message();self.status=status
        for key,value in headers:self.headers[key]=value
    def geturl(self):return store.model.SOURCES['mmf']['url']

class Opener:
    def __init__(self,response):self.response=response;self.requests=[]
    def open(self,request,timeout):self.requests.append((request,timeout));return self.response

def http(raw):
    class Socket:
        def makefile(self,*args,**kwargs):return BytesIO(raw)
    out=HTTPResponse(Socket());out.begin();out.url=store.model.SOURCES['mmf']['url'];return out

def obtain(response):
    opener=Opener(response)
    with patch.object(store.urllib.request,'build_opener',return_value=opener):result=store.acquire('mmf')
    assert len(opener.requests)==1 and opener.requests[0][1]==30
    return result

class Tests(unittest.TestCase):
    def test_fragmented_originals_are_whole_and_do_not_require_one_allocation_per_fragment(self):
        raw=bytes(range(256))*1024;stream=Fragmented(raw,137)
        self.assertEqual(store.bounded(stream,expected_length=len(raw)),raw);self.assertTrue(stream.closed);self.assertGreater(stream.calls,1000)

    def test_byte_bounds_empty_nonbinary_oversized_fragments_and_declared_lengths(self):
        class Nonbinary(Fragmented):
            def read(self,n=-1):return 'invented'
        class Oversized(Fragmented):
            def read(self,n=-1):return b'x'*(n+1)
        for stream in (Fragmented(b''),Nonbinary(b'x'),Oversized(b'x'),Fragmented(b'x'*17)):
            with patch.object(store,'MAX',16),self.assertRaises(ValueError):store.bounded(stream)
            self.assertTrue(stream.closed)
        for length in (0,True,-1,1.0,'1',18):
            stream=Fragmented(b'x')
            with patch.object(store,'MAX',16),self.assertRaises(ValueError):store.bounded(stream,expected_length=length)
            self.assertTrue(stream.closed)
        for raw in (b'x',b'x'*16):
            stream=Fragmented(raw)
            with patch.object(store,'MAX',16):self.assertEqual(store.bounded(stream,expected_length=len(raw)),raw)

    def test_s3_requires_typed_whole_content_length_and_closes_on_every_rejection(self):
        for length in (None,True,0,-1,'4',4.0,3,5):
            stream=Fragmented(b'body')
            with self.assertRaises(ValueError):store.stored({'Body':stream,'ContentLength':length})
            self.assertTrue(stream.closed)
        stream=Fragmented(b'body');self.assertEqual(store.stored({'Body':stream,'ContentLength':4}),b'body');self.assertTrue(stream.closed)

    def test_read_errors_and_elapsed_limit_close_the_stream(self):
        class Broken(Fragmented):
            def read(self,n=-1):raise OSError('invented transport failure')
        stream=Broken(b'body')
        with self.assertRaises(OSError):store.bounded(stream)
        self.assertTrue(stream.closed)
        for clock in ([0,30],[0,0,30]):
            stream=Fragmented(b'body')
            with patch.object(store.time,'monotonic',side_effect=clock),self.assertRaises(ValueError):store.bounded(stream)
            self.assertTrue(stream.closed)

    def test_real_http_parser_accepts_complete_length_chunked_and_connection_close_bodies(self):
        body=b'complete invented table body'
        for raw in (b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body,
                    b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n'+hex(len(body))[2:].encode()+b'\r\n'+body+b'\r\n0\r\n\r\n',
                    b'HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n'+body):
            response=http(raw);out,meta=obtain(response);self.assertEqual(out,body);self.assertEqual(meta['http_status'],200);self.assertTrue(response.closed)

    def test_real_http_parser_rejects_truncated_length_and_unterminated_chunks(self):
        for raw in (b'HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nshort',
                    b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nshort\r\n'):
            response=http(raw)
            with self.assertRaises((ValueError,IncompleteRead)):obtain(response)
            self.assertTrue(response.closed)

    def test_partial_ambiguous_or_encoded_http_metadata_refuses_before_body_read(self):
        cases=[([],206),([],True),([],199),([],600),([('Content-Range','bytes 0-3/8')],200),
          ([('Content-Encoding','gzip')],200),([('Content-Encoding','identity'),('Content-Encoding','identity')],200),
          ([('Content-Length','4'),('Content-Length','4')],200),([('Content-Length','4,4')],200),([('Content-Length','+4')],200),
          ([('Content-Length','4'),('Transfer-Encoding','chunked')],200),([('Transfer-Encoding','gzip')],200)]
        for headers,status in cases:
            response=Response(b'body',headers,status)
            with self.assertRaises(ValueError):obtain(response)
            self.assertTrue(response.closed);self.assertEqual(response.calls,0)

    def test_complete_http_errors_are_preserved_with_one_attempt_and_no_redirect(self):
        response=Response(b'<html>invented 429 body</html>',status=429);raw,meta=obtain(response)
        self.assertEqual(raw,b'<html>invented 429 body</html>');self.assertEqual(meta['http_status'],429);self.assertTrue(response.closed)
        response=Response(b'body');response.geturl=lambda:'https://unreviewed.invalid/'
        with self.assertRaises(ValueError):obtain(response)
        self.assertTrue(response.closed);self.assertEqual(response.calls,0)

    def test_complete_native_run_and_all_storage_readbacks_accept_fragmentation(self):
        class FragmentedMemory(Memory):
            def get_object(self,**kw):
                out=super().get_object(**kw);raw=out['Body'].read();out['Body'].close();out['Body']=Fragmented(raw);return out
        mem=FragmentedMemory();mem.data['data/history/ici-mmf.json']=b'{"legacy":[1,2,3]}'
        with patch.object(store,'acquire',side_effect=acquire),patch.object(store,'now',return_value=GENERATED):result=store.run(mem,'invented','event')
        self.assertTrue(result['published']);self.assertEqual(result['original_arithmetic_checks']['observation_checks'],81)
        self.assertEqual(result['original_arithmetic_checks']['independent_rational_reconciliations'],48)
        self.assertEqual(mem.data['data/history/ici-mmf.json'],b'{"legacy":[1,2,3]}')
        store.replay(store.strict(mem.data[store.CURRENT]),store.reader(mem,'invented'))

    def test_whole_native_outputs_and_returns_equal_predecessor_under_equal_clocks_and_compilers(self):
        raw=(ROOT/'tests/fixtures/pre-ici-complete-transport-store.py.txt').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),'49f88eb8582333b478bbfedb2823ed859d1ebfe79a624907d627be3b7f0880a4')
        old=ModuleType('ici_pre_transport');old.__file__=str(store.ROOT/'ici_store.py');exec(compile(raw,old.__file__,'exec'),old.__dict__)
        before=Memory();after=Memory()
        for mem in (before,after):mem.data['data/history/ici-mmf.json']=b'{"whole_legacy":[1,2,3]}'
        with patch.object(old,'acquire',side_effect=acquire),patch.object(old,'now',return_value=GENERATED):old_result=old.run(before,'invented','same-event')
        with patch.object(store,'acquire',side_effect=acquire),patch.object(store,'now',return_value=GENERATED):new_result=store.run(after,'invented','same-event')
        self.assertEqual(old_result,new_result);self.assertEqual(before.data,after.data)

    def test_failed_error_journal_does_not_mask_original_source_failure(self):
        class FailedJournal(Memory):
            def put_object(self,**kw):
                if kw['Key'].startswith(store.PRIVATE+'requests/') and store.strict(kw['Body']).get('status')=='failed':raise StorageError('AccessDenied')
                return super().put_object(**kw)
        mem=FailedJournal();cause=TimeoutError('invented original source failure')
        with patch.object(store,'acquire',side_effect=cause),self.assertRaises(TimeoutError) as caught:store.run(mem,'invented','journal-failure')
        self.assertIs(caught.exception,cause);self.assertEqual(cause.__notes__,['ICI failure journal could not be verified']);self.assertNotIn(store.CURRENT,mem.data)

if __name__=='__main__':unittest.main()
