"""Complete native S3 bodies, including the SDK EOF length check."""
import io,json,sys,time,unittest
from pathlib import Path
from unittest.mock import patch
from botocore.response import StreamingBody
from botocore.exceptions import IncompleteReadError
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import extremes_native_store as store
from extremes_native_test_support import Storage,fixture,packet,STAMP


class Pieces(io.BytesIO):
    def read(self,n=-1):return super().read(min(n,3))


class Transport(unittest.TestCase):
    def test_fragmented_complete_bytes_are_not_parsed_as_prefix(self):
        raw=b'{"whole":[0,null,false,"retained"]}';stream=Pieces(raw)
        self.assertEqual(store.bounded(stream),raw);self.assertTrue(stream.closed)
    def test_actual_sdk_rejects_incomplete_body_that_looks_like_valid_json(self):
        raw=io.BytesIO(b'{}');stream=StreamingBody(raw,100)
        with self.assertRaises(IncompleteReadError):store.bounded(stream)
        self.assertTrue(raw.closed)
    def test_complete_sdk_body_reaches_eof_and_preserves_bytes(self):
        raw=b'{"n":0,"available":false,"x":null}';source=io.BytesIO(raw)
        self.assertEqual(store.bounded(StreamingBody(source,len(raw))),raw);self.assertTrue(source.closed)
    def test_empty_body_and_exact_limit_are_distinct_from_oversize(self):
        with patch.object(store,'MAX',8):
            for raw in (b'',b'12345678'):
                stream=Pieces(raw);self.assertEqual(store.bounded(stream),raw);self.assertTrue(stream.closed)
            stream=Pieces(b'123456789')
            with self.assertRaises(ValueError):store.bounded(stream)
            self.assertTrue(stream.closed)
    def test_stream_error_and_nonbyte_response_close_without_success(self):
        class Broken:
            closed=False;count=0
            def read(self,n):
                self.count+=1
                if self.count==1:return b'{}'
                raise OSError('synthetic read failure')
            def close(self):self.closed=True
        stream=Broken()
        with self.assertRaises(OSError):store.bounded(stream)
        self.assertTrue(stream.closed)
        for value in (None,'{}',False,bytearray(b'{}')):
            stream=Broken();stream.read=lambda n:value
            with self.assertRaises(ValueError):store.bounded(stream)
            self.assertTrue(stream.closed)
    def test_public_reader_rejects_incomplete_existing_object(self):
        s=Storage();key=store.current('capitulation')
        with patch.object(s,'get_object',side_effect=lambda **kw:{'Body':StreamingBody(io.BytesIO(b'{}'),100)}):
            with self.assertRaises(IncompleteReadError):store.reader(s,'synthetic')(key)
        self.assertEqual(s.writes,[])
    def test_capture_does_not_retain_or_interpret_incomplete_upstream(self):
        s=Storage()
        with patch.object(s,'get_object',side_effect=lambda **kw:{'Body':StreamingBody(io.BytesIO(b'{}'),100)}):
            with self.assertRaises(IncompleteReadError):store.collect(s,'synthetic','capitulation',time.monotonic()+30)
        self.assertEqual(s.writes,[])
    def test_publish_preserves_current_when_predecessor_transfer_is_incomplete(self):
        s,_,p=packet();key=store.current('capitulation');s.objects[key]=b'{}';before=dict(s.objects);writes=len(s.writes);get=s.get_object
        def read(**request):
            if request['Key']==key:
                raw=s.objects[key];return {'Body':StreamingBody(io.BytesIO(raw),len(raw)+98),'ETag':store.model.sha(raw)}
            return get(**request)
        with patch.object(s,'get_object',side_effect=read):
            with self.assertRaises(IncompleteReadError):store.publish(s,'synthetic',p)
        self.assertEqual(s.objects,before);self.assertEqual(len(s.writes),writes)
    def test_failed_capture_only_records_failed_request_and_keeps_public_current(self):
        s,_=fixture();key=store.current('capitulation');s.objects[key]=b'{"existing":true}';before=s.objects[key];get=s.get_object
        upstream=set(store.model.SOURCES[name][0] for name in store.model.INPUTS['capitulation'])
        def read(**request):
            if request['Key'] in upstream:return {'Body':StreamingBody(io.BytesIO(b'{}'),100)}
            return get(**request)
        with patch.object(s,'get_object',side_effect=read),patch.object(store,'now',return_value=STAMP):
            with self.assertRaises(RuntimeError):store.run(s,'synthetic','capitulation','short-transfer','synthetic-execution')
        self.assertEqual(s.objects[key],before)
        status=json.loads(s.objects[store.request_key('capitulation','short-transfer')]);self.assertEqual(status['status'],'failed')
    def test_immutable_readback_must_receive_the_whole_object(self):
        s=Storage();raw=b'{}';key=store.PREFIX+'inputs/'+store.model.sha(raw)+'.json'
        with patch.object(s,'get_object',return_value={'Body':StreamingBody(io.BytesIO(raw),100)}):
            with self.assertRaises(IncompleteReadError):store.immutable(s,'synthetic',key,raw)
        self.assertEqual(s.objects[key],raw)


def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Transport))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
