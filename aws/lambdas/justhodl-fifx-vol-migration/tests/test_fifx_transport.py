"""Complete invented HTTP/storage responses and exact old-archive replay."""
from copy import deepcopy
from email.message import Message
from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,io,json,unittest,urllib.error
import run_tests as native

store,acquisition,model=native.store,native.acquisition,native.model
HERE=Path(__file__).resolve().parent
URL='https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10&cosd=1988-01-01&coed=2026-09-26'


class Body:
    def __init__(self,raw,fragment=7,headers=(),status=200,url=URL):
        self.raw=raw;self.offset=0;self.fragment=fragment;self.headers=Message()
        self.code=status;self.url=url;self.closed=0;self.reads=[]
        for key,value in headers:self.headers[key]=value
    def read(self,n):
        self.reads.append(n);size=min(n,self.fragment,len(self.raw)-self.offset)
        out=self.raw[self.offset:self.offset+size];self.offset+=size;return out
    def close(self):self.closed+=1
    def geturl(self):return self.url


class Tests(unittest.TestCase):
    def test_full_fragmented_binary_stream_and_exact_bound(self):
        raw=bytes(range(256))*5001;body=Body(raw,fragment=17393)
        self.assertEqual(store.bounded(body,expected_length=len(raw)),raw)
        self.assertEqual(body.closed,1);self.assertGreater(len(body.reads),2)
        self.assertEqual(store.bounded(Body(b'x'*64),limit=64),b'x'*64)
        body=Body(b'x'*65)
        with self.assertRaises(ValueError):store.bounded(body,limit=64)
        self.assertEqual(body.closed,1)

    def test_invalid_length_fragment_or_interruption_always_closes(self):
        for length in (True,0,-1,'8',2,20):
            body=Body(b'complete')
            with self.assertRaises(ValueError):store.bounded(body,expected_length=length)
            self.assertEqual(body.closed,1)
        for value in (None,'x',bytearray(b'x'),b'x'*100):
            body=Body(b'');body.read=lambda n,value=value:value
            with self.assertRaises(ValueError):store.bounded(body,limit=64)
            self.assertEqual(body.closed,1)
        body=Body(b'')
        def fail(n):raise OSError('invented interrupted response')
        body.read=fail
        with self.assertRaises(OSError):store.bounded(body)
        self.assertEqual(body.closed,1)

    def test_s3_requires_typed_complete_length_and_gzip_checks_stored_bytes(self):
        raw=b'{"whole_invented_original":[1,2,3,null,false]}\n'
        for length in (None,True,'48',len(raw)-1,len(raw)+1):
            body=Body(raw)
            with self.assertRaises(ValueError):store.stored({'Body':body,'ContentLength':length})
            self.assertEqual(body.closed,1)
        body=Body(raw);self.assertEqual(store.stored({'Body':body,'ContentLength':len(raw)}),raw)
        client=native.Store();key='data/evidence/fred/'+64*'a'+'/'+store.sha(raw)+'.bin.gz'
        client.objects[key]=gzip.compress(raw,mtime=0)
        self.assertEqual(store.reader(client,'bucket')(key),raw)
        original=client.get_object
        def short(**kw):
            response=original(**kw);response['ContentLength']+=1;return response
        with patch.object(client,'get_object',side_effect=short),self.assertRaises(ValueError):store.reader(client,'bucket')(key)

    def test_http_preserves_whole_source_bytes_with_or_without_declared_length(self):
        raw=native.fixture.csv('DGS10',n=42)
        for headers in ([],[('Content-Length',str(len(raw)))],[('Content-Length','000'+str(len(raw)))]):
            body=Body(raw,headers=[*headers,('Content-Type','text/csv'),('ETag','invented'),('Set-Cookie','invented-private-cookie')]);requests=[]
            class Opener:
                def open(self,request,timeout):requests.append((request,timeout));return body
            with patch.object(acquisition.urllib.request,'build_opener',return_value=Opener()):actual,receipt=acquisition.acquire(URL)
            self.assertEqual(actual,raw);self.assertEqual(receipt['bytes'],len(raw));self.assertEqual(receipt['sha256'],store.sha(raw))
            self.assertEqual(receipt['source_url'],URL);self.assertEqual(receipt['http_status'],200)
            self.assertNotIn('Set-Cookie',receipt['headers']);self.assertEqual(body.closed,1)
            self.assertEqual(len(requests),1);self.assertEqual(requests[0][0].get_header('Accept-encoding'),'identity')

    def test_partial_redirect_encoded_and_ambiguous_length_responses_refuse(self):
        bodies=[Body(b'whole',status=206),Body(b'whole',status=True),Body(b'whole',url='https://invented.invalid'),
                Body(b'whole',headers=[('Content-Range','bytes 0-4/100')]),Body(b'whole',headers=[('Content-Encoding','gzip')])]
        bodies += [Body(b'whole',headers=headers) for headers in ([('Content-Length','7')],[('Content-Length','4')],[('Content-Length','0')],
            [('Content-Length','-5')],[('Content-Length','5,5')],[('Content-Length','5'),('Content-Length','5')])]
        for body in bodies:
            opener=type('Opener',(),{'open':lambda *a,**kw:body})()
            with patch.object(acquisition.urllib.request,'build_opener',return_value=opener),self.assertRaises(ValueError):acquisition.acquire(URL)
            self.assertEqual(body.closed,1)
        with patch.object(acquisition.urllib.request,'build_opener') as opener,self.assertRaises(ValueError):acquisition.acquire(URL,timeout=True)
        opener.assert_not_called()

    def test_complete_http_error_body_is_retained_without_retry(self):
        raw=b'{"whole_invented_provider_error":"unavailable"}'
        body=Body(raw);headers=Message();headers['Content-Length']=str(len(raw));headers['Content-Type']='application/json'
        error=urllib.error.HTTPError(URL,503,'unavailable',headers,body)
        opener=type('Opener',(),{'open':lambda *a,**kw:(_ for _ in ()).throw(error)})()
        with patch.object(acquisition.urllib.request,'build_opener',return_value=opener):actual,receipt=acquisition.acquire(URL)
        self.assertEqual(actual,raw);self.assertEqual(receipt['http_status'],503);self.assertEqual(receipt['bytes'],len(raw));self.assertEqual(body.closed,1)

    def test_fragmented_native_storage_replays_every_whole_source(self):
        client=native.Store();original=client.get_object;streams=[]
        def fragments(**request):
            response=original(**request);raw=response['Body'].read();response['Body'].close()
            body=Body(raw,fragment=137);streams.append(body);return {**response,'Body':body}
        with patch.object(client,'get_object',side_effect=fragments):
            packet,_,defs=native.packet(client)
            with patch.object(store,'definitions',return_value=defs):proofs=store.replay(packet,store.reader(client,'bucket'))
        self.assertEqual(len(proofs),18);self.assertEqual(sum(row['original_rows'] for row in proofs.values()),756)
        self.assertTrue(all(body.closed==1 for body in streams))

    def test_partial_predecessor_cannot_be_preserved_or_published(self):
        client=native.Store();packet,_,_=native.packet(client)
        client.objects[model.CURRENT]=model.encoded({'generated_at':'2026-09-25T00:00:00Z','whole_invented_predecessor':[1,2,3]})+b'\n '
        before=deepcopy(client.objects);writes=len(client.puts);original=client.get_object;streams=[]
        def short(**request):
            response=original(**request)
            if request['Key']==model.CURRENT:
                raw=response['Body'].read();response['Body'].close();body=Body(raw[:-1]);streams.append(body);response['Body']=body
            return response
        with patch.object(client,'get_object',side_effect=short):
            with self.assertRaises(ValueError):store.previous_state(client,'bucket')
            with self.assertRaises(ValueError):store.publish(client,'bucket',packet)
        self.assertEqual(client.objects,before);self.assertEqual(len(client.puts),writes);self.assertTrue(all(body.closed==1 for body in streams))

    def test_partial_immutable_acknowledgement_is_not_success(self):
        client=native.Store();original=client.get_object
        def wrong(**request):
            response=original(**request);response['ContentLength']+=1;return response
        with patch.object(client,'get_object',side_effect=wrong),self.assertRaises(ValueError):
            store.retain_bytes(client,'bucket',b'whole invented original','originals','bin')
        self.assertNotIn(model.CURRENT,client.objects);self.assertNotIn(model.HISTORY,client.objects)

    def test_only_exact_transport_predecessors_replay_in_their_original_slots(self):
        client=native.Store();packet,_,defs=native.packet(client);read=store.reader(client,'bucket');manifest=store.binding(packet,read)
        hashes={'fifx_store':'46421fb3b919245412532b1410366524eda9ba6b1cb46214dff6de6183afe6f7',
                'fifx_acquire':'810bba6d56ce5d32c7072034d7bcc828f63000c681fdb8cc5148f2b191e566ec'}
        legacy={name:(HERE/('pre_complete_transport_'+name+'.py.txt')).read_bytes() for name in hashes}
        for name,raw in legacy.items():self.assertEqual(hashlib.sha256(raw).hexdigest(),hashes[name])
        def version(changes):
            changed=deepcopy(manifest)
            for name,raw in changes.items():changed['compilers'][name]=store.retain_bytes(client,'bucket',raw,'compilers','py')
            ref=store.retain_bytes(client,'bucket',model.encoded(changed),'runs')
            return {**packet,'replay':{**packet['replay'],'manifest_key':ref['key']}}
        with patch.object(store,'definitions',return_value=defs):
            self.assertEqual(store.replay(version(legacy),read),store.replay(packet,read))
            for changes in ({'fifx_store':legacy['fifx_store']+b'\n'}, {'fifx_acquire':legacy['fifx_acquire']+b'\n'},
                            {'fifx_candidate':legacy['fifx_store']},{'fifx_store':legacy['fifx_acquire']}):
                with self.assertRaises(ValueError):store.replay(version(changes),read)


if __name__=='__main__':unittest.main(verbosity=2)
