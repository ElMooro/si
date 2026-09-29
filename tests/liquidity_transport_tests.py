"""Complete liquidity transport and reviewed compiler compatibility; offline only."""
import gzip,io,json,sys,unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from botocore.response import StreamingBody
from botocore.exceptions import IncompleteReadError
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-liquidity-flow/tests')]
import liquidity_flow_store as store
import calls_research_replay as calls
from test_liquidity_native import setup,Storage,STAMP

PRED='4e7c99fe5eabdae3380cbdbbb7ecf6d65d25928c21b64c36b373402924336533'
FIXTURE=ROOT/'tests/fixtures/pre-liquidity-transport-store.py.txt'


class Pieces(io.BytesIO):
    def read(self,n=-1):return super().read(min(n,3))


def packet(client,inputs):
    out=store.compile_output(inputs,store.reader(client,'synthetic'))
    return {**out,'replay':store.retain(client,'synthetic',inputs,out)}


def predecessor(client,value):
    manifest=store.binding(value,store.reader(client,'synthetic'));raw=FIXTURE.read_bytes()
    assert store.sha(raw)==PRED
    key=store.model.PREFIX+'compilers/'+PRED+'.py';client.objects[key]=raw
    manifest['compilers']['liquidity_flow_store']={'key':key,'sha256':PRED}
    ref=store.retain_bytes(client,'synthetic',store.model.encoded(manifest),'runs')
    return {**deepcopy(value),'replay':{'manifest_key':ref['key'],'output_sha256':manifest['output_sha256']}},key


def rehash(bundle):
    bundle['payload_sha256']=calls.digest(bundle['payload']);bundle['run_id']='calls-research-'+bundle['payload_sha256']
    return bundle


class Transport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.objects,cls.inputs,_=setup()
    def test_short_reads_join_through_eof_without_changing_explicit_types(self):
        raw=b'{"n":0,"x":null,"available":false}';stream=Pieces(raw)
        self.assertEqual(store.bounded(stream),raw);self.assertTrue(stream.closed)
    def test_real_sdk_rejects_a_valid_json_prefix_with_incomplete_content_length(self):
        body=io.BytesIO(b'{}')
        with self.assertRaises(IncompleteReadError):store.bounded(StreamingBody(body,100))
        self.assertTrue(body.closed)
    def test_real_sdk_rejects_more_bytes_than_declared(self):
        body=io.BytesIO(b'{} ')
        with self.assertRaises(IncompleteReadError):store.bounded(StreamingBody(body,2))
        self.assertTrue(body.closed)
    def test_complete_sdk_empty_and_fragmented_bodies_reach_eof(self):
        for raw in (b'',b'{}',b'{"whole":[0,null,false]}'):
            body=Pieces(raw);self.assertEqual(store.bounded(StreamingBody(body,len(raw))),raw);self.assertTrue(body.closed)
    def test_exact_bound_is_complete_and_over_bound_never_returns_a_prefix(self):
        with patch.object(store,'MAX',8):
            body=Pieces(b'12345678');self.assertEqual(store.bounded(body),b'12345678');self.assertTrue(body.closed)
            body=Pieces(b'123456789')
            with self.assertRaises(ValueError):store.bounded(body)
            self.assertTrue(body.closed)
    def test_failure_after_valid_prefix_and_invalid_stream_types_always_close(self):
        class Broken:
            closed=False;count=0
            def read(self,n):
                self.count+=1
                if self.count==1:return b'{}'
                raise OSError('synthetic transport failure')
            def close(self):self.closed=True
        body=Broken()
        with self.assertRaises(OSError):store.bounded(body)
        self.assertTrue(body.closed)
        for invalid in (None,'{}',False,bytearray(b'{}')):
            body=Broken();body.read=lambda n:invalid
            with self.assertRaises(ValueError):store.bounded(body)
            self.assertTrue(body.closed)
    def test_reader_drains_transport_and_all_gzip_members(self):
        key='data/evidence/fred/'+'a'*64+'/'+'b'*64+'.bin.gz'
        raw=gzip.compress(b'{"whole":')+gzip.compress(b'[0,null,false]}')
        client=Storage({key:raw});bodies=[]
        def get(**kw):
            body=Pieces(client.objects[kw['Key']]);bodies.append(body)
            return {'Body':StreamingBody(body,len(raw))}
        with patch.object(client,'get_object',side_effect=get):self.assertEqual(store.reader(client,'synthetic')(key),b'{"whole":[0,null,false]}')
        self.assertTrue(all(b.closed for b in bodies))
    def test_reader_rejects_truncated_or_corrupt_gzip_even_when_json_is_complete(self):
        key='data/evidence/fred/'+'a'*64+'/'+'b'*64+'.bin.gz';raw=gzip.compress(b'{}')
        for bad in (raw[:-4],raw[:-8]+b'\0'*8):
            client=Storage({key:bad})
            with self.assertRaises((EOFError,OSError)):store.reader(client,'synthetic')(key)
            self.assertEqual(client.writes,[])
    def test_uncompressed_bound_is_enforced_separately_from_transport_bound(self):
        key='data/evidence/fred/'+'a'*64+'/'+'b'*64+'.bin.gz';raw=gzip.compress(b'x'*129)
        with patch.object(store,'MAX',128):
            self.assertLess(len(raw),store.MAX)
            with self.assertRaises(ValueError):store.reader(Storage({key:raw}),'synthetic')(key)
    def test_immutable_write_cannot_claim_success_after_incomplete_readback(self):
        client=Storage({});raw=b'{}';key=store.model.PREFIX+'inputs/'+store.sha(raw)+'.json'
        with patch.object(client,'get_object',return_value={'Body':StreamingBody(io.BytesIO(raw),100)}):
            with self.assertRaises(IncompleteReadError):store.immutable(client,'synthetic',key,raw)
        self.assertEqual(client.objects[key],raw)
    def test_predecessor_incomplete_transfer_does_not_archive_or_replace_it(self):
        client=Storage({store.model.CURRENT:b'{}'});before=dict(client.objects)
        with patch.object(client,'get_object',return_value={'Body':StreamingBody(io.BytesIO(b'{}'),100)}):
            with self.assertRaises(IncompleteReadError):store.previous_context(client,'synthetic')
        self.assertEqual(client.objects,before);self.assertEqual(client.writes,[])
    def test_incomplete_macro_capture_preserves_existing_publication_without_writes(self):
        client=Storage({**self.objects,store.model.CURRENT:b'{"existing":true}'});before=dict(client.objects);get=client.get_object
        def read(**kw):
            if kw['Key']==store.SOURCE:return {'Body':StreamingBody(io.BytesIO(b'{}'),100)}
            return get(**kw)
        with patch.object(client,'get_object',side_effect=read):
            with self.assertRaises(IncompleteReadError):store.run(client,'synthetic')
        self.assertEqual(client.objects,before);self.assertEqual(client.writes,[])
    def test_optional_settlement_incomplete_transfer_is_unavailable_not_retained(self):
        client=Storage({});read=store.reader(client,'synthetic')
        with patch.object(client,'get_object',return_value={'Body':StreamingBody(io.BytesIO(b'{}'),100)}):
            ref,status=store.settlement_input(client,'synthetic',read)
        self.assertIsNone(ref);self.assertEqual(status,{'status':'unavailable','reason':'SOURCE_READ_FAILED'});self.assertEqual(client.writes,[])
    def test_public_writer_preserves_predecessor_when_complete_read_cannot_be_proven(self):
        client=Storage(self.objects);value=packet(client,self.inputs);client.objects[store.model.CURRENT]=b'{}';before=dict(client.objects);count=len(client.writes);get=client.get_object
        def read(**kw):
            if kw['Key']==store.model.CURRENT:return {'Body':StreamingBody(io.BytesIO(b'{}'),100),'ETag':'synthetic'}
            return get(**kw)
        with patch.object(client,'get_object',side_effect=read):
            with self.assertRaises(IncompleteReadError):store.publish(client,'synthetic',value)
        self.assertEqual(client.objects,before);self.assertEqual(len(client.writes),count)
    def test_fragmented_whole_storage_path_retains_and_replays_exact_arithmetic(self):
        client=Storage(self.objects);get=client.get_object
        def read(**kw):
            result=get(**kw);raw=result['Body'].read();result['Body'].close();result['Body']=Pieces(raw);return result
        with patch.object(client,'get_object',side_effect=read):
            value=packet(client,self.inputs);out=store.replay(value['replay'],store.reader(client,'synthetic'))
        self.assertTrue(store.same_json(out,{k:v for k,v in value.items() if k!='replay'}));self.assertFalse(out['sizing_eligible'])


class Compatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.objects,cls.inputs,_=setup()
    def test_reviewed_store_predecessor_reproduces_without_executing_archived_code(self):
        client=Storage(self.objects);value,key=predecessor(client,packet(client,self.inputs));read=store.reader(client,'synthetic')
        self.assertIn(PRED,store.REVIEWED_STORAGE_REVISIONS)
        self.assertTrue(store.same_json(store.replay(value['replay'],read),{k:v for k,v in value.items() if k!='replay'}))
        client.objects[key]+=b'\n'
        with self.assertRaisesRegex(ValueError,'predecessor storage bytes'):store.replay(value['replay'],read)
    def test_actual_prechange_frozen_calls_fixture_still_reproduces_exactly(self):
        bundle=json.loads((ROOT/'tests/fixtures/pre-liquidity-transport-calls-bundle.json').read_bytes());before=calls.canonical(bundle)
        result=calls.replay(bundle)
        self.assertEqual(result['status'],'reproduced');self.assertEqual(result['compiler_match'],'reviewed_storage_transport_revision')
        self.assertEqual(result['output_sha256'],calls.digest(bundle['payload']['output']));self.assertEqual(calls.canonical(bundle),before)
        self.assertFalse(result['sizing_eligible']);self.assertEqual(result['call_verb'],'WAIT')
    def test_compatibility_requires_exact_reviewed_pair_and_every_other_compiler(self):
        original=json.loads((ROOT/'tests/fixtures/pre-liquidity-transport-calls-bundle.json').read_bytes())
        for field,value in (('liquidity_flow_store.py','a'*64),('calls_free_brief.py','b'*64),('calls_research_replay.py',calls.compiler_identity()['files']['calls_research_replay.py'])):
            bad=deepcopy(original);bad['payload']['compiler']['files'][field]=value
            with self.assertRaisesRegex(ValueError,'compiler version differs'):calls.replay(rehash(bad))
        for field,value in (('method','unreviewed'),('source_hash_basis','other')):
            bad=deepcopy(original);bad['payload']['compiler'][field]=value
            with self.assertRaisesRegex(ValueError,'compiler version differs'):calls.replay(rehash(bad))
        bad=deepcopy(original);bad['payload']['compiler']['files']['unexpected.py']='c'*64
        with self.assertRaisesRegex(ValueError,'compiler version differs'):calls.replay(rehash(bad))
    def test_compatible_identity_does_not_excuse_changed_output_or_missing_inputs(self):
        original=json.loads((ROOT/'tests/fixtures/pre-liquidity-transport-calls-bundle.json').read_bytes())
        bad=deepcopy(original);bad['payload']['output']['brief_md']+=' altered conclusion'
        with self.assertRaisesRegex(ValueError,'reproduced brief differs'):calls.replay(rehash(bad))
        bad=deepcopy(original);bad['payload']['inputs'].pop('data/ciss-stress.json')
        with self.assertRaisesRegex(ValueError,'incomplete input'):calls.replay(rehash(bad))
    def test_new_calls_bundle_binds_current_code_and_old_liquidity_originals_exactly(self):
        client=Storage(self.objects);value,_=predecessor(client,packet(client,self.inputs));read=store.reader(client,'synthetic')
        bundle=calls.prepare(lambda key:value if key==store.model.CURRENT else {},STAMP,read)
        self.assertEqual(bundle['payload']['inputs'][store.model.CURRENT]['original_binding']['status'],'verified')
        self.assertEqual(bundle['payload']['compiler'],calls.compiler_identity())
        result=calls.replay(bundle,read);self.assertEqual(result['compiler_match'],'exact');self.assertEqual(result['status'],'reproduced')
        self.assertFalse(bundle['payload']['output']['sizing_eligible']);self.assertEqual(result['call_verb'],'WAIT')


def run():
    suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(cls) for cls in (Transport,Compatibility))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
