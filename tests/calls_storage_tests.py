"""Complete stored-byte comparisons, strict publication transport and historical replay."""
from pathlib import Path
from unittest.mock import patch
import copy,io,json,sys,unittest
from botocore.response import StreamingBody
from botocore.exceptions import IncompleteReadError
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import calls_research_replay as calls
AT='2026-09-29T20:00:00Z';OLD='2026-09-29T19:00:00Z'

class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Small(io.BytesIO):
    def read(self,n=-1):return super().read(min(n,7) if n>=0 else 7)

class Store:
    def __init__(self,raw=None,code='PreconditionFailed',body=None,length=None):
        self.raw=raw;self.code=code;self.body=body or io.BytesIO(raw or b'');self.length=len(raw or b'') if length is None else length;self.writes=[];self.reads=[]
    def put_object(self,**kw):
        self.writes.append(kw)
        if self.code:raise StorageError(self.code)
        self.raw=kw['Body']
    def get_object(self,**kw):
        self.reads.append(kw);return {'Body':self.body,'ContentLength':self.length,'ETag':'v1'}

def bundle():return calls.prepare(lambda key:{},AT)
def publish(store,document=None):return calls.publish_current(store,'synthetic','synthetic-current',document or {'generated_at':AT,'zero':0,'missing':None})

class Storage(unittest.TestCase):
    def test_new_immutable_record_has_exact_canonical_bytes_and_conditional_identity(self):
        value=bundle();before=copy.deepcopy(value);s=Store(code=None);result=calls.persist(s,'synthetic',value)
        self.assertEqual(value,before);self.assertEqual(s.reads,[]);self.assertEqual(s.raw,calls.canonical(value))
        self.assertEqual(s.writes[0]['IfNoneMatch'],'*');self.assertEqual(result['bundle_key'],calls.PREFIX+value['payload_sha256']+'.json')
        self.assertEqual(result['bundle_sha256'],calls.hashlib.sha256(s.raw).hexdigest())
    def test_all_supported_conditional_conflicts_close_exact_existing_response(self):
        value=bundle();raw=calls.canonical(value)
        for code in ('PreconditionFailed','412','ConditionalRequestConflict','409'):
            with self.subTest(code=code):
                s=Store(raw,code);result=calls.persist(s,'synthetic',value)
                self.assertEqual(result['status'],'retained');self.assertTrue(s.body.closed)
                self.assertEqual(len(s.writes),1);self.assertEqual(s.reads,[{'Bucket':'synthetic','Key':result['bundle_key']}])
    def test_byte_conflict_including_whitespace_closes_without_rewriting(self):
        value=bundle();raw=calls.canonical(value)
        for old in (raw+b' ',raw.replace(b'WAIT',b'HOLD',1),b'{}'):
            s=Store(old)
            with self.assertRaisesRegex(ValueError,'immutable'):calls.persist(s,'synthetic',value)
            self.assertTrue(s.body.closed);self.assertEqual(s.raw,old);self.assertEqual(len(s.writes),1)
    def test_fragmented_sdk_body_reaches_eof_before_identical_retry_is_acknowledged(self):
        value=bundle();raw=calls.canonical(value);stream=Small(raw)
        s=Store(raw,body=StreamingBody(stream,len(raw)))
        self.assertEqual(calls.persist(s,'synthetic',value)['status'],'retained');self.assertTrue(stream.closed)
    def test_incomplete_sdk_conflict_response_cannot_claim_retained(self):
        value=bundle();raw=calls.canonical(value);stream=io.BytesIO(raw)
        s=Store(raw,body=StreamingBody(stream,len(raw)+1))
        with self.assertRaises(IncompleteReadError):calls.persist(s,'synthetic',value)
        self.assertTrue(stream.closed);self.assertEqual(len(s.writes),1)
    def test_invalid_declared_length_cannot_claim_immutable_retention(self):
        value=bundle();raw=calls.canonical(value)
        for length in (True,-1,str(len(raw)),len(raw)+1,len(raw)-1):
            s=Store(raw,length=length)
            with self.assertRaisesRegex(ValueError,'length'):calls.persist(s,'synthetic',value)
            self.assertTrue(s.body.closed);self.assertEqual(len(s.writes),1)
    def test_read_errors_and_nonbyte_chunks_close_and_never_claim_retention(self):
        value=bundle();raw=calls.canonical(value)
        class Broken(io.BytesIO):
            def read(self,*args):raise OSError('synthetic interrupted read')
        class Text(io.BytesIO):
            def read(self,*args):return 'not bytes'
        for stream,error in ((Broken(raw),OSError),(Text(raw),ValueError)):
            s=Store(raw,body=stream)
            with self.assertRaises(error):calls.persist(s,'synthetic',value)
            self.assertTrue(stream.closed);self.assertEqual(len(s.writes),1)
    def test_nonconflict_write_failure_does_not_fetch_or_acknowledge_a_record(self):
        s=Store(code='AccessDenied')
        with self.assertRaises(StorageError):calls.persist(s,'synthetic',bundle())
        self.assertEqual(s.reads,[]);self.assertEqual(len(s.writes),1)
    def test_partial_current_object_is_read_completely_before_conditional_update(self):
        raw=calls.canonical({'generated_at':OLD,'zero':0});stream=Small(raw);s=Store(raw,code=None,body=StreamingBody(stream,len(raw)))
        self.assertTrue(publish(s)['published']);self.assertTrue(stream.closed)
        self.assertEqual(s.writes[0]['IfMatch'],'v1');self.assertEqual(json.loads(s.raw)['zero'],0)
    def test_current_length_mismatch_prevents_any_replacement_and_closes(self):
        raw=calls.canonical({'generated_at':OLD})
        for length in (True,-1,str(len(raw)),len(raw)+1,len(raw)-1):
            s=Store(raw,code=None,length=length)
            with self.assertRaisesRegex(ValueError,'length'):publish(s)
            self.assertTrue(s.body.closed);self.assertEqual(s.raw,raw);self.assertEqual(s.writes,[])
    def test_stored_current_requires_utf8_json_without_bom_or_invalid_surrogates(self):
        text=json.dumps({'generated_at':OLD})
        for raw in (text.encode('utf-16'),b'\xef\xbb\xbf'+text.encode(),b'{"generated_at":"'+OLD.encode()+b'","x":"\\ud800"}'):
            s=Store(raw,code=None)
            with self.assertRaises((ValueError,UnicodeError)):publish(s)
            self.assertTrue(s.body.closed);self.assertEqual(s.writes,[])
    def test_complete_large_current_object_is_not_silently_truncated(self):
        value={'generated_at':AT,'description':'evidence'*40000,'zero':0,'missing':None};raw=calls.canonical(value)
        s=Store(raw,code=None)
        self.assertEqual(publish(s,value),{'published':True,'reason':'already_current'})
        self.assertTrue(s.body.closed);self.assertEqual(s.writes,[]);self.assertEqual(s.raw,raw)
    def test_same_clock_fragmented_identical_object_is_no_write_without_clock_renewal(self):
        value={'generated_at':AT,'zero':0,'missing':None};raw=json.dumps(value,indent=2).encode();s=Store(raw,code=None,body=Small(raw))
        self.assertEqual(publish(s,value),{'published':True,'reason':'already_current'})
        self.assertTrue(s.body.closed);self.assertEqual(s.writes,[]);self.assertEqual(s.raw,raw)

class Compatibility(unittest.TestCase):
    def test_four_exact_retained_compiler_sets_replay_with_current_source(self):
        for name in ('pre-liquidity-transport-calls-bundle.json','pre-calls-publication-bundle.json','pre-calls-cache-bundle.json','pre-calls-storage-bundle.json'):
            value=json.loads((ROOT/'tests/fixtures'/name).read_bytes());before=calls.canonical(value)
            result=calls.replay(value);self.assertEqual(result['status'],'reproduced');self.assertEqual(result['compiler_match'],'reviewed_storage_transport_revision')
            self.assertEqual(calls.canonical(value),before);self.assertFalse(result['sizing_eligible'])
    def test_unknown_mixed_or_changed_record_cannot_use_transport_compatibility(self):
        value=json.loads((ROOT/'tests/fixtures/pre-calls-storage-bundle.json').read_bytes())
        for edit in (lambda b:b['payload']['compiler']['files'].update({'calls_research_replay.py':'0'*64}),
                     lambda b:b['payload']['compiler']['files'].update({'calls_original_reader.py':'7470b5bb9890f2438cee3a7cee2658595347770df79835e15415c7db9f543bae'}),
                     lambda b:b['payload']['output'].update({'call_verb':'LONG'})):
            bad=copy.deepcopy(value);edit(bad);bad['payload_sha256']=calls.digest(bad['payload']);bad['run_id']='calls-research-'+bad['payload_sha256']
            with self.assertRaises(ValueError):calls.replay(bad)

def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(cls) for cls in (Storage,Compatibility)))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
