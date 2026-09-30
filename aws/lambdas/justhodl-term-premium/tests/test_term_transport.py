"""Complete invented transport and preserved whole-workbook archive replay."""
from copy import deepcopy
from email.message import Message
from pathlib import Path
from unittest.mock import patch
import hashlib,io,json,sys,unittest,urllib.error
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE));import test_term_native as native
store,model=native.store,native.model


class Body:
    def __init__(self,raw,fragment=7,headers=(),status=200,url=None):
        self.raw=raw;self.offset=0;self.fragment=fragment;self.closed=0;self.reads=[]
        self.headers=Message();self.status=status;self.url=url or model.arithmetic.URL
        for key,value in headers:self.headers[key]=value
    def read(self,n):
        self.reads.append(n);size=min(n,self.fragment,len(self.raw)-self.offset)
        out=self.raw[self.offset:self.offset+size];self.offset+=size;return out
    def close(self):self.closed+=1
    def geturl(self):return self.url


class Tests(unittest.TestCase):
    def test_complete_fragmented_binary_body_and_exact_bound(self):
        raw=bytes(range(256))*5001
        stream=Body(raw,fragment=17393)
        self.assertEqual(store.bounded(stream,expected_length=len(raw)),raw)
        self.assertEqual(stream.closed,1);self.assertGreater(len(stream.reads),2)
        with patch.object(store,'MAX',64):
            self.assertEqual(store.bounded(Body(b'x'*64)),b'x'*64)
            too_large=Body(b'x'*65)
            with self.assertRaises(ValueError):store.bounded(too_large)
            self.assertEqual(too_large.closed,1)

    def test_missing_bytes_invalid_fragments_and_read_errors_close_and_refuse(self):
        for length in (True,0,-1,'10',2,20):
            body=Body(b'complete')
            with self.assertRaises(ValueError):store.bounded(body,expected_length=length)
            self.assertEqual(body.closed,1)
        for value in (None,'x',bytearray(b'x'),b'x'*100):
            body=Body(b'');body.read=lambda n,value=value:value
            with patch.object(store,'MAX',64),self.assertRaises(ValueError):store.bounded(body)
            self.assertEqual(body.closed,1)
        body=Body(b'')
        def fail(n):raise OSError('invented interrupted transport')
        body.read=fail
        with self.assertRaises(OSError):store.bounded(body)
        self.assertEqual(body.closed,1)

    def test_s3_declared_length_is_required_and_never_treated_as_a_boolean(self):
        raw=b'{"whole_invented_observation":1}\n  '
        body=Body(raw)
        self.assertEqual(store.stored({'Body':body,'ContentLength':len(raw)}),raw)
        for length in (None,True,str(len(raw)),len(raw)-1,len(raw)+1):
            body=Body(raw)
            with self.assertRaises(ValueError):store.stored({'Body':body,'ContentLength':length})
            self.assertEqual(body.closed,1)

    def test_http_full_body_and_original_source_identity_are_preserved(self):
        raw=b'whole invented workbook'*1001
        for headers in ([],[('Content-Length',str(len(raw)))],[('Content-Length','000'+str(len(raw))) ]):
            body=Body(raw,fragment=103,headers=[*headers,('Content-Type','application/vnd.ms-excel'),('ETag','invented-whole')])
            with patch.object(store.urllib.request,'urlopen',return_value=body),patch.object(store,'now',return_value=native.NOW):
                actual,source=store.acquire()
            self.assertEqual(actual,raw);self.assertEqual(source,{'source_url':model.arithmetic.URL,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'acquired_at':native.NOW,'response_headers':{'Content-Type':'application/vnd.ms-excel','ETag':'invented-whole','Last-Modified':None}})
            self.assertEqual(body.closed,1)

    def test_partial_status_range_redirect_encoding_and_length_errors_refuse(self):
        bodies=[Body(b'whole',status=206),Body(b'whole',status=204),Body(b'whole',url='https://invented.invalid/redirect'),
                Body(b'whole',headers=[('Content-Range','bytes 0-4/100')]),Body(b'whole',headers=[('Content-Encoding','gzip')])]
        bodies += [Body(b'whole',headers=headers) for headers in ([('Content-Length','7')],[('Content-Length','4')],[('Content-Length','0')],[('Content-Length','-5')],[('Content-Length','5,5')],[('Content-Length','5'),('Content-Length','5')])]
        for body in bodies:
            with self.subTest(headers=list(body.headers.items()),status=body.status),patch.object(store.urllib.request,'urlopen',return_value=body),self.assertRaises(ValueError):store.acquire()
            self.assertEqual(body.closed,1)
        fp=io.BytesIO(b'invented upstream failure')
        error=urllib.error.HTTPError(model.arithmetic.URL,503,'unavailable',{},fp)
        with patch.object(store.urllib.request,'urlopen',side_effect=error),self.assertRaises(urllib.error.HTTPError):store.acquire()
        self.assertTrue(fp.closed)

    def test_fragmented_stored_workbook_replays_all_original_rows(self):
        case=native.Tests();case.setUp();self.addCleanup(case.doCleanups)
        original=case.client.get_object;streams=[]
        def get(**request):
            obj=original(**request);raw=obj['Body'].read();obj['Body'].close()
            body=Body(raw,fragment=137);streams.append(body);return {**obj,'Body':body}
        with patch.object(case.client,'get_object',side_effect=get):
            packet=case.packet();actual=store.replay(packet,case.read)
        self.assertEqual(actual,case.output);self.assertEqual(actual['original_arithmetic_checks']['original_rows'],320)
        self.assertTrue(all(body.closed==1 for body in streams))

    def test_incomplete_predecessor_and_retention_readback_cannot_publish(self):
        case=native.Tests();case.setUp();self.addCleanup(case.doCleanups)
        packet=case.packet();before=deepcopy(case.client.objects);original=case.client.get_object;streams=[]
        def get(**request):
            obj=original(**request)
            if request['Key']==model.CURRENT:
                raw=obj['Body'].read();obj['Body'].close();body=Body(raw[:-1]);streams.append(body);obj['Body']=body
            return obj
        with patch.object(case.client,'get_object',side_effect=get):
            with self.assertRaises(ValueError):store.previous_state(case.client,'b')
            with self.assertRaises(ValueError):store.publish(case.client,'b',packet)
        self.assertEqual(before,case.client.objects);self.assertTrue(all(body.closed==1 for body in streams))
        # A complete immutable put with a partial acknowledgement stays an error.
        def bad_ack(**request):
            obj=original(**request);obj['ContentLength']+=1;return obj
        with patch.object(case.client,'get_object',side_effect=bad_ack),self.assertRaises(ValueError):
            store.retain_bytes(case.client,'b',b'whole invented body','sources')

    def test_exact_predecessor_transport_compiler_replays_but_any_other_change_fails(self):
        case=native.Tests();case.setUp();self.addCleanup(case.doCleanups)
        packet=case.packet();manifest=store.binding(packet,case.read)
        old=(HERE/'pre_complete_transport_store.py.txt').read_bytes()
        self.assertEqual(hashlib.sha256(old).hexdigest(),'def1ed96c885267e1ea17a5761fcbef94a8cfe62a6ce67dddca89f806d350828')
        def version(raw,name='term_premium_store.py'):
            changed=deepcopy(manifest);changed['compilers'][name]=store.retain_bytes(case.client,'b',raw,'compilers','py')
            ref=store.retain_bytes(case.client,'b',model.encoded(changed),'runs')
            return {**packet,'replay':{**packet['replay'],'manifest_key':ref['key']}}
        prior=version(old)
        self.assertEqual(store.replay(prior,case.read),case.output)
        with self.assertRaises(ValueError):store.replay(version(old+b'\n'),case.read)
        with self.assertRaises(ValueError):store.replay(version(old,'term_premium_model.py'),case.read)


if __name__=='__main__':unittest.main(verbosity=2)
