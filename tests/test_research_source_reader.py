"""Whole-object transport boundaries; fixture data never reaches production."""
from pathlib import Path
from io import BytesIO
import gzip,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import research_source_reader as m

class Client:
    def __init__(self,raw,encoding=None,length=None,partial=False):
        class Body(BytesIO):
            def read(self,n=-1):return super().read(min(n,17) if partial else n)
        self.body=Body(raw);self.calls=0
        self.obj={'Body':self.body,'ContentLength':len(raw) if length is None else length}
        if encoding is not None:self.obj['ContentEncoding']=encoding
    def get_object(self,**kwargs):self.calls+=1;return self.obj

class Tests(unittest.TestCase):
    def test_complete_large_body_includes_tail_and_hashes_all_stored_bytes(self):
        raw=json.dumps({'large':'x'*8_000_000,'last':{'value':0}}).encode();client=Client(raw)
        doc,sha,received=m.read(client,'b','data/example.json')
        self.assertEqual(doc['last'],{'value':0});self.assertEqual(len(doc['large']),8_000_000)
        self.assertEqual(sha,hashlib.sha256(raw).hexdigest());self.assertTrue(client.body.closed)
        self.assertTrue(received.endswith('+00:00'));self.assertEqual(m.policy()['stored_byte_limit'],32*1024*1024)
    def test_short_stream_reads_and_gzip_keep_wire_identity_and_real_zero(self):
        raw=b'{"zero":0,"missing":null,"last":[1,2,3]}'
        for encoding,body in ((None,raw),(None,gzip.compress(raw)),('gzip',gzip.compress(raw))):
            client=Client(body,encoding,partial=True);doc,sha,_=m.read(client,'b','data/example.json')
            self.assertEqual(doc,json.loads(raw));self.assertEqual(sha,hashlib.sha256(body).hexdigest());self.assertTrue(client.body.closed)
    def test_stored_length_encoding_and_complete_gzip_member_are_required(self):
        raw=b'{"a":1}';gz=gzip.compress(raw)
        for client in (Client(raw,length=99),Client(raw,'gzip'),Client(gz,'identity'),Client(gz,'br'),
                       Client(gz[:-3],'gzip'),Client(gz+gz,'gzip'),Client(gz+b'tail','gzip'),Client(gz[:-1]+b'x','gzip')):
            with self.assertRaises(ValueError):m.read(client,'b','data/example.json')
            self.assertTrue(client.body.closed)
    def test_independent_stored_and_decoded_limits_close_the_body(self):
        old=m.MAX_BYTES;m.MAX_BYTES=1000
        try:
            for client,reason in ((Client(b' '*1001),'SOURCE_EXCEEDS_CAPTURE_BOUND'),
                                  (Client(gzip.compress(b'{"a":"'+b'x'*1100+b'"}'),'gzip'),'SOURCE_EXCEEDS_DECODED_BOUND')):
                with self.assertRaisesRegex(ValueError,reason):m.read(client,'b','data/example.json')
                self.assertTrue(client.body.closed)
        finally:m.MAX_BYTES=old
    def test_invalid_json_is_not_a_source_outage_or_partial_projection(self):
        for raw in (b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":Infinity}',b'{"a":1e999}',b'\xff',b'{"unfinished":',b''):
            client=Client(raw)
            with self.assertRaisesRegex(ValueError,'SOURCE_INVALID_JSON'):m.read(client,'b','data/example.json')
            self.assertTrue(client.body.closed)
        with self.assertRaisesRegex(ValueError,'UNSUPPORTED_SOURCE_SHAPE'):m.read(Client(b'[]'),'b','data/example.json')
    def test_denied_stream_closes_and_private_sources_are_never_requested(self):
        client=Client(b'{}')
        with self.assertRaisesRegex(ValueError,'private'):m.read(client,'b','data/private/account.json')
        self.assertEqual(client.calls,0)
        client=Client(b'{}')
        def fail(*args):raise OSError('transport stopped')
        client.body.read=fail
        with self.assertRaises(OSError):m.read(client,'b','data/example.json')
        self.assertTrue(client.body.closed)

if __name__=='__main__':unittest.main()
