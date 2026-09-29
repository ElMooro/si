"""Complete CB response transport using only synthetic stores and providers."""
from pathlib import Path
from unittest.mock import patch
import base64,gzip,io,json,sys,unittest
sys.path.insert(0,str(Path(__file__).parent))
import test_research as support
from botocore.response import StreamingBody
store=support.store


class Chunks(io.BytesIO):
    def __init__(self,raw,chunk=7):super().__init__(raw);self.chunk=chunk;self.requests=[]
    def read(self,n=-1):
        self.requests.append(n)
        return super().read(min(n,self.chunk) if n>=0 else self.chunk)


class TrackedStore(support.Storage):
    def __init__(self):super().__init__();self.bodies=[];self.writes=[]
    def get_object(self,**kw):
        out=super().get_object(**kw);raw=out['Body'].read();out['Body'].close();body=Chunks(raw)
        self.bodies.append(body);return {**out,'Body':body,'ContentLength':len(raw)}
    def put_object(self,**kw):self.writes.append(kw['Key']);return super().put_object(**kw)


class Response(Chunks):
    def __init__(self,raw,length=None):super().__init__(raw);self.headers={} if length is None else {'Content-Length':length}


class Transport(unittest.TestCase):
    def test_complete_short_chunks_and_empty_bodies_reach_eof_and_close(self):
        for raw in (b'',b'complete retained evidence'*10000):
            body=Chunks(raw,23);self.assertEqual(store.bounded(body),raw);self.assertTrue(body.closed)
            self.assertTrue(all(0<n<=65536 for n in body.requests))
    def test_exact_limit_is_complete_and_one_more_byte_is_rejected(self):
        for raw,valid in ((b'x'*32,True),(b'x'*33,False)):
            body=Chunks(raw)
            if valid:self.assertEqual(store.bounded(body,limit=32),raw)
            else:
                with self.assertRaisesRegex(ValueError,'bound'):store.bounded(body,limit=32)
            self.assertTrue(body.closed)
    def test_sdk_incomplete_transfer_is_not_accepted_as_a_complete_object(self):
        inner=io.BytesIO(b'{"legacy":true}');body=StreamingBody(inner,99)
        with self.assertRaises(Exception) as caught:store.bounded(body)
        self.assertEqual(type(caught.exception).__name__,'IncompleteReadError');self.assertTrue(inner.closed)
    def test_declared_lengths_are_strict_and_must_match_the_complete_body(self):
        for declared in (True,-1,2.0,'2',0,1,3):
            body=Chunks(b'{}')
            with self.assertRaises(ValueError):store.bounded(body,expected_length=declared)
            self.assertTrue(body.closed)
        body=Chunks(b'{}');self.assertEqual(store.bounded(body,expected_length=2),b'{}');self.assertTrue(body.closed)
    def test_errors_and_nonbyte_streams_close_without_returning_a_prefix(self):
        for kind in ('error','str','none'):
            class Bad(Chunks):
                def read(self,n=-1):
                    if kind=='error':raise OSError('synthetic stream error')
                    return 'x' if kind=='str' else None
            body=Bad(b'x')
            with self.assertRaises((ValueError,OSError)):store.bounded(body)
            self.assertTrue(body.closed)
    def test_object_reader_checks_declared_transfer_length_before_parsing(self):
        body=Chunks(b'{}');client=type('Client',(),{'get_object':lambda *a,**kw:{'Body':body,'ContentLength':99}})()
        with self.assertRaises(ValueError):store.raw_reader(client,'synthetic')('data/synthetic.json')
        self.assertTrue(body.closed)
    def test_compressed_originals_are_complete_and_invalid_gzip_fails(self):
        raw=b'retained original observations'*5000;c=TrackedStore();c.objects['data/synthetic.bin.gz']=gzip.compress(raw)
        self.assertEqual(store.raw_reader(c,'synthetic')('data/synthetic.bin.gz'),raw);self.assertTrue(all(b.closed for b in c.bodies))
        c.objects['data/synthetic.bin.gz']=c.objects['data/synthetic.bin.gz'][:-4]
        with self.assertRaises((EOFError,OSError)):store.raw_reader(c,'synthetic')('data/synthetic.bin.gz')
        self.assertTrue(all(b.closed for b in c.bodies))
    def test_immutable_retry_compares_the_whole_object_not_an_equal_prefix(self):
        body=Chunks(b'{} extra bytes',2)
        class Client:
            def put_object(self,**kw):raise support.StorageError('PreconditionFailed')
            def get_object(self,**kw):return {'Body':body,'ContentLength':14}
        with self.assertRaisesRegex(ValueError,'immutable research differs'):store.immutable(Client(),'synthetic','data/synthetic.json',b'{}')
        self.assertTrue(body.closed)
    def test_current_publication_rejects_incomplete_transfer_without_writing(self):
        old={'contract':support.model.CONTRACT,'generated_at':support.NOW,'source_generated_at':support.NOW}
        raw=support.encoded(old);body=Chunks(raw);writes=[]
        client=type('Client',(),{'get_object':lambda *a,**kw:{'Body':body,'ETag':'old','ContentLength':len(raw)+1},'put_object':lambda *a,**kw:writes.append(kw)})()
        with self.assertRaises(ValueError):store.publish(client,'synthetic',store.CURRENT,{**old,'generated_at':'2026-09-20T00:00:00Z'})
        self.assertEqual(writes,[]);self.assertTrue(body.closed)
    def test_missing_etag_closes_current_and_cache_bodies(self):
        for operation in ('publish','cache'):
            body=Chunks(b'{}');client=type('Client',(),{'get_object':lambda *a,**kw:{'Body':body,'ContentLength':2}})()
            with self.assertRaises(KeyError):
                if operation=='publish':store.publish(client,'synthetic',store.CURRENT,{})
                else:store.acquire_ecb(client,'synthetic','total_assets',lambda _:self.fail('Unexpected original read'))
            self.assertTrue(body.closed)
    def test_ecb_capture_keeps_all_bytes_and_the_response_completion_clock(self):
        seed=support.Storage();raw=support.ecb_fixture(seed,'total_assets')['raw'];body=Response(raw,str(len(raw)))
        opener=type('Opener',(),{'open':lambda *a,**kw:body})();c=TrackedStore()
        with patch.object(store,'now',return_value=support.NOW),patch.object(store.urllib.request,'build_opener',return_value=opener):
            out,error=store.acquire_ecb(c,'synthetic','total_assets',store.raw_reader(c,'synthetic'))
        self.assertIsNone(error);self.assertEqual(out['raw'],raw);self.assertTrue(body.closed)
        self.assertEqual(out['evidence']['first_received_at'],out['acquired_at']);self.assertEqual(out['evidence']['bytes'],len(raw))
    def test_ecb_bad_length_is_not_captured_or_written(self):
        for header in ('99','-1','1, 1','1.0',''):
            body=Response(b'{}',header);opener=type('Opener',(),{'open':lambda *a,**kw:body})();c=TrackedStore()
            with patch.object(store.urllib.request,'build_opener',return_value=opener),patch.object(store.evidence_store,'capture',side_effect=AssertionError('Incomplete bytes captured')):
                with self.assertRaises(ValueError):store.acquire_ecb(c,'synthetic','total_assets',store.raw_reader(c,'synthetic'))
            self.assertTrue(body.closed);self.assertEqual(c.writes,[])
    def test_ecb_incomplete_response_falls_back_without_renewing_cached_acquisition(self):
        c=TrackedStore();item=support.ecb_fixture(c,'total_assets');item['acquired_at']='2026-09-16T00:00:00Z'
        c.objects[store.PREFIX+'ecb-cache/total_assets.json']=support.encoded({k:v for k,v in item.items() if k!='raw'})
        body=Response(b'{}','999');opener=type('Opener',(),{'open':lambda *a,**kw:body})()
        with patch.object(store,'now',return_value=support.NOW),patch.object(store.urllib.request,'build_opener',return_value=opener):
            out,error=store.acquire_ecb(c,'synthetic','total_assets',store.raw_reader(c,'synthetic'))
        self.assertEqual(error,'ValueError');self.assertEqual(out['raw'],item['raw']);self.assertEqual(out['acquired_at'],item['acquired_at']);self.assertTrue(body.closed)
    def test_current_run_and_genuine_pre_edit_run_replay_with_complete_chunked_storage(self):
        c,source,original,ecb=support.prepared();tracked=TrackedStore();tracked.objects=dict(c.objects);tracked.metadata=dict(c.metadata)
        with patch.object(store,'now',return_value=support.NOW),patch.object(store,'acquire_ecb',side_effect=lambda client,bucket,name,read:(ecb[name],None)):
            result=store.run(tracked,'synthetic')
        self.assertTrue(result['published']);self.assertTrue(all(b.closed for b in tracked.bodies))
        root=Path(store.__file__).resolve().parents[4]
        frozen=json.loads((root/'tests/fixtures/pre-cb-transport-native.json').read_bytes());old=TrackedStore()
        old.objects={k:base64.b64decode(v,validate=True) for k,v in frozen['objects'].items()};packet=frozen['packet'];before=dict(old.objects)
        out=support.replay(json.loads(old.objects[packet['replay']['manifest_key']]),store.raw_reader(old,'synthetic'))
        self.assertEqual(out,{k:v for k,v in packet.items() if k!='replay'});self.assertEqual(old.objects,before);self.assertEqual(old.writes,[])
        self.assertIsNone(out['call']);self.assertIsNone(out['global_injection_impulse']['score']);self.assertFalse(out['sizing_eligible'])
        self.assertTrue(all(b.closed for b in old.bodies))


if __name__=='__main__':unittest.main()
