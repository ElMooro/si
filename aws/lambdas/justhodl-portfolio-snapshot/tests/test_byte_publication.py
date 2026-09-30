"""Complete invented bodies and controlled transport only; no external I/O."""
from pathlib import Path
from email.message import Message
from unittest.mock import patch
import base64,copy,gzip,hashlib,importlib.util,io,json,math,os,sys,types,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'aws/shared'))
SOURCE=ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source/portfolio_snapshot_publication.py'

class Response(io.BytesIO):
    def __init__(self,raw,headers=None,status=200,url=None):
        super().__init__(raw);self.headers=Message();self.status=status;self.url=url or 'https://justhodl-data-proxy.raafouis.workers.dev/private-artifact?kind=portfolio-snapshot';self.sizes=[]
        for key,value in (headers or [('Content-Type','application/json'),('Content-Length',str(len(raw)))]):self.headers[key]=value
    def geturl(self):return self.url
    def read(self,n=-1):self.sizes.append(n);return super().read(n)

class Publication(unittest.TestCase):
    def setUp(self):
        spec=importlib.util.spec_from_file_location('reviewed_snapshot_publication',SOURCE);self.mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.mod)
        self.secret_calls=[];self.calls=[];self.body=b'{"positions":[],"number":-0.0}';self.identity=42
        self.env=patch.dict(os.environ,{},clear=False);self.env.start();self.addCleanup(self.env.stop);os.environ.pop('PRIVATE_ARTIFACT_PROXY',None)
        self.mod.service_headers=lambda:self.secret_calls.append(True) or {'X-JH-Service-Token':'invented-publisher-463'}
        self.net=patch('socket.socket.connect',side_effect=AssertionError('External networking forbidden'));self.net.start();self.addCleanup(self.net.stop)
    def ack(self,**extra):return {'ok':True,'protocol':self.mod.PROTOCOL,'body_bytes':len(self.body),'body_sha256':hashlib.sha256(self.body).hexdigest(),'identity_bytes':self.identity,**extra}
    def send(self,value=None,raw=None,headers=None,response=None,context=None):
        response=response or Response(raw if raw is not None else json.dumps(self.ack() if value is None else value).encode(),headers)
        self.response=response
        def open_(request,**kwargs):self.calls.append((request,kwargs));return response
        with patch.object(self.mod.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=open_)) as factory:
            value=self.mod.publish_snapshot(self.body,self.identity,context)
        self.factory_call=factory.call_args
        return value
    def test_exact_compact_bytes_for_full_cross_runtime_vectors(self):
        values=[r['value'] for r in json.loads((ROOT/'tests/fixtures/portfolio-value-identity-vectors.json').read_bytes())]
        values += [-0.0,2**53-1,-(2**53-1),5e-324,1e-300,False,None,{'whole': ['π 😀','\\\"\n\t\b\f\r',0.00001]}]
        for value in values:self.assertEqual(self.mod.encode_snapshot(value),json.dumps(value,separators=(',',':'),allow_nan=False).encode())
    def test_complete_native_frames_match_every_original_mocked_s3_body(self):
        frames=json.loads(gzip.decompress((ROOT/'tests/fixtures/portfolio-sector-browser-synthetic.json.gz').read_bytes()))
        names=['complete','partial','binary','empty','mixed','known'];self.assertEqual([n for n,r in frames['cases'].items() if 'writes' in r],names)
        for name in names:
            encoded=frames['cases'][name]['writes'][1]['request']['Body'];raw=base64.b64decode(encoded['body'],validate=True)
            self.assertEqual(hashlib.sha256(raw).hexdigest(),encoded['sha256']);self.assertEqual(len(raw),encoded['bytes']);self.assertEqual(self.mod.encode_snapshot(json.loads(raw)),raw)
    def test_all_string_chunk_edges_keep_complete_escaping_and_unicode(self):
        for size in (4095,4096,4097,8191,8192):
            value={'x':('a'*size)+'😀\n\0\\雪'};self.assertEqual(self.mod.encode_snapshot(value),json.dumps(value,separators=(',',':')).encode())
    def test_exact_wire_byte_boundary_and_one_byte_over(self):
        value={'n':'😀\0'};raw=json.dumps(value,separators=(',',':')).encode();self.mod.MAX_BYTES=len(raw)
        self.assertEqual(self.mod.encode_snapshot(value),raw);self.mod.MAX_BYTES-=1
        with self.assertRaisesRegex(ValueError,'byte bound'):self.mod.encode_snapshot(value)
    def test_nesting_limit_and_cycles_are_bounded(self):
        value=None
        for _ in range(128):value=[value]
        self.assertEqual(json.loads(self.mod.encode_snapshot(value)),value)
        for invalid in ([value],):
            with self.assertRaisesRegex(ValueError,'nesting bound'):self.mod.encode_snapshot(invalid)
        cyclic=[];cyclic.append(cyclic)
        with self.assertRaisesRegex(ValueError,'nesting bound'):self.mod.encode_snapshot(cyclic)
    def test_unsafe_types_numbers_and_unicode_are_never_coerced_or_quoted_in_errors(self):
        from decimal import Decimal
        for value in (10**400,2**53,float(2**53),float('nan'),float('inf'),Decimal('1'),(1,),{1:0},b'{}','INVENTED_PRIVATE\ud800'):
            with self.assertRaises(ValueError) as failure:self.mod.encode_snapshot(value)
            self.assertNotIn('INVENTED_PRIVATE',str(failure.exception))
    def test_complete_body_is_shared_with_request_and_exact_acknowledgement_required(self):
        self.assertEqual(self.send(),self.ack());request,options=self.calls[0]
        self.assertIs(request.data,self.body);self.assertEqual(request.method,'PUT');self.assertEqual(request.full_url,self.mod.URL)
        self.assertEqual(request.get_header('X-jh-body-sha256'),hashlib.sha256(self.body).hexdigest());self.assertGreater(options['timeout'],0);self.assertLessEqual(options['timeout'],8)
        self.assertIsInstance(self.factory_call.args[0],self.mod.NoRedirect);self.assertTrue(self.response.closed);self.assertTrue(all(0<n<=1024 for n in self.response.sizes))
    def test_acknowledgement_cannot_substitute_bool_number_hash_protocol_or_success(self):
        for change in ({'ok':1},{'ok':False},{'protocol':'legacy'},{'body_bytes':True},{'body_bytes':len(self.body)+1},{'body_sha256':'0'*64},{'identity_bytes':True},{'identity_bytes':self.identity+1}):
            with self.assertRaisesRegex(self.mod.SnapshotPublicationUnavailable,'does not match'):self.send(value=self.ack(**change))
            self.assertTrue(self.response.closed)
    def test_acknowledgement_requires_complete_strict_json(self):
        for raw in (b'{"ok":true,"ok":false}',b'{"x":NaN}',b'{"x":1e999}',b'{"x":"\\ud800"}',b'\xff',b'{"ok":',b'\xef\xbb\xbf{}'):
            with self.assertRaisesRegex(self.mod.SnapshotPublicationUnavailable,'invalid'):self.send(raw=raw)
            self.assertTrue(self.response.closed)
    def test_acknowledgement_length_encoding_type_and_duplicates_are_checked(self):
        valid=('Content-Type','application/json')
        for headers in ([valid,('Content-Length','1')],[valid,('Content-Length','4097')],[valid,('Content-Length','2'),('Content-Length','2')],[valid,('Content-Encoding','gzip')],[('Content-Type','text/plain')],[valid,valid],[valid,('Content-Length','+1')]):
            with self.assertRaises(self.mod.SnapshotPublicationUnavailable):self.send(headers=headers)
            self.assertTrue(self.response.closed)
    def test_unbounded_and_oversized_acknowledgements_cannot_be_read(self):
        with self.assertRaisesRegex(self.mod.SnapshotPublicationUnavailable,'byte bound'):self.send(raw=b' '*4097,headers=[('Content-Type','application/json')])
        self.assertTrue(self.response.closed);self.assertLessEqual(sum(self.response.sizes),4097)
    def test_status_or_final_url_change_refuses_publication(self):
        for response in (Response(b'{}',status=201),Response(b'{}',url='https://other.invalid/')):
            with self.assertRaisesRegex(self.mod.SnapshotPublicationUnavailable,'response not accepted'):self.send(response=response)
            self.assertTrue(response.closed)
    def test_redirect_closes_transport_without_forwarding_credentials(self):
        fp=io.BytesIO(b'INVENTED_PRIVATE')
        with self.assertRaisesRegex(self.mod.SnapshotPublicationUnavailable,'redirect refused'):self.mod.NoRedirect().redirect_request(None,fp,302,'',{},'https://other.invalid')
        self.assertTrue(fp.closed)
    def test_alternate_origin_refused_before_secret_or_network_access(self):
        for origin in ('http://justhodl-data-proxy.raafouis.workers.dev','https://other.invalid','https://justhodl-data-proxy.raafouis.workers.dev@other.invalid'):
            os.environ['PRIVATE_ARTIFACT_PROXY']=origin
            with self.assertRaisesRegex(self.mod.SnapshotPublicationUnavailable,'origin required'):self.mod.publish_snapshot(self.body,self.identity)
        self.assertEqual(self.secret_calls,[])
    def test_absent_or_insufficient_runtime_budget_prevents_both_secret_and_transport(self):
        for value in (True,None,10**400,float('inf'),0,5000,5999):
            with self.assertRaises(self.mod.SnapshotPublicationUnavailable):self.mod.publish_snapshot(self.body,self.identity,types.SimpleNamespace(get_remaining_time_in_millis=lambda:value))
        self.assertEqual(self.secret_calls,[])
    def test_context_reserves_time_for_the_second_sink(self):
        self.send(context=types.SimpleNamespace(get_remaining_time_in_millis=lambda:6500));self.assertLessEqual(self.calls[0][1]['timeout'],1.5)
    def test_late_acknowledgement_is_not_accepted(self):
        ticks=iter([0,0,0,0,30,30,30])
        with patch.object(self.mod.time,'monotonic',side_effect=lambda:next(ticks)):
            with self.assertRaisesRegex(self.mod.SnapshotPublicationUnavailable,'deadline'):self.send()
        self.assertTrue(self.response.closed)
    def test_transport_failures_never_copy_private_exception_text(self):
        opener=types.SimpleNamespace(open=lambda *a,**kw:(_ for _ in ()).throw(RuntimeError('INVENTED_PRIVATE')))
        with patch.object(self.mod.urllib.request,'build_opener',return_value=opener):
            with self.assertRaises(self.mod.SnapshotPublicationUnavailable) as error:self.mod.publish_snapshot(self.body,self.identity)
        self.assertNotIn('INVENTED_PRIVATE',str(error.exception))
    def test_raw_body_and_identity_type_limits_precede_authentication(self):
        for body,count in (('',1),(bytearray(b'{}'),1),(b'',1),(b'{}',True),(b'{}',0),(b'{}',2**53)):
            with self.assertRaises(self.mod.SnapshotPublicationUnavailable):self.mod.publish_snapshot(body,count)
        self.assertEqual(self.secret_calls,[])

if __name__=='__main__':unittest.main()
