from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
import gzip,hashlib,io,json,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).resolve().parents[1]/'source')]
import plumbing_research_store as store
import plumbing_research_model as model
from test_native_research import FIX,AT


class FakeError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.writes=[];self.reads=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body'];self.writes.append(kw)


def fixture():
    source=json.loads((FIX/'macro.json').read_bytes());mm=json.loads((FIX/'macro-manifest.json').read_bytes())
    funding=json.loads((FIX/'funding.json').read_bytes());fm=json.loads((FIX/'funding-manifest.json').read_bytes())
    inputs=json.loads((FIX/'funding-input.json').read_bytes())
    objects={'data/report-measurements.json':model.encoded(source),'data/eurodollar-plumbing.json':model.encoded(funding),
        source['replay']['manifest_key']:model.encoded(mm),funding['replay']['manifest_key']:model.encoded(fm),
        mm['compiler']['key']:Path(store.report_observations.__file__).read_bytes(),
        fm['input']['key']:model.encoded(inputs),fm['output']['key']:model.encoded({k:v for k,v in funding.items() if k!='replay'})}
    for sid in model.SERIES:
        if sid not in source['measurements']:continue
        for ref in mm['inputs'][sid]['evidence'].values():objects[ref['key']]=gzip.compress((FIX/'originals'/ref['sha256']).read_bytes(),mtime=0)
    ref=inputs['originals']['ofr_fsi']['evidence'];objects[ref['key']]=gzip.compress((FIX/'originals'/ref['sha256']).read_bytes(),mtime=0)
    return objects,{'contract':'plumbing-inputs.v1','generated_at':AT,'macro':source,'funding':funding,'context':{}}


class ReplayAndPublication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.objects,cls.inputs=fixture();storage=Storage(cls.objects)
        cls.output,cls.histories=store.compile_output(cls.inputs,store.reader(storage,'b'))
    def setUp(self):self.s=Storage(self.objects);self.read=store.reader(self.s,'b')

    def test_complete_real_fixture_has_portable_digest(self):
        self.assertEqual(model.digest(self.output),(FIX/'expected-output.sha256').read_text().strip())

    def test_immutable_originals_compilers_and_all_history_shards_replay(self):
        ref=store.retain(self.s,'b',self.inputs,self.output,self.histories)
        self.assertEqual(store.replay(ref,self.read),self.output)
        self.assertEqual(len(self.histories),54)
        key=next(iter(self.histories));self.s.objects[key]=b'{}'
        with self.assertRaisesRegex(ValueError,'history shard'):store.replay(ref,self.read)

    def test_original_csv_tampering_stops_before_publication(self):
        raw=json.loads((FIX/'funding-input.json').read_bytes());ref=raw['originals']['ofr_fsi']['evidence']
        self.s.objects[ref['key']]=gzip.compress(b'altered',mtime=0)
        with self.assertRaisesRegex(ValueError,'original source bytes'):store.compile_output(self.inputs,self.read)
        self.assertNotIn(model.CURRENT,self.s.objects)

    def test_whole_carrier_and_original_input_bindings_are_required(self):
        bad=deepcopy(self.inputs);bad['funding']['measurements']['ofr_fsi:ofr_fsi']['value_decimal']='99'
        with self.assertRaisesRegex(ValueError,'upstream packet'):store.compile_output(bad,self.read)
        manifest=json.loads(self.read(self.inputs['funding']['replay']['manifest_key']))
        self.s.objects[manifest['input']['key']]=b'{}'
        with self.assertRaisesRegex(ValueError,'artifact content'):store.compile_output(self.inputs,self.read)

    def test_wrong_request_and_future_receipt_are_rejected(self):
        ref=deepcopy(self.inputs['macro']['measurements']['SOFR']['evidence']['observations'])
        with self.assertRaisesRegex(ValueError,'request differs'):store.evidence(ref,'fred','https://example.org',AT,self.read)
        ref['first_received_at']='2027-01-01T00:00:00Z'
        with self.assertRaisesRegex(ValueError,'receipt is future'):store.evidence(ref,'fred',ref['source_url'],AT,self.read)

    def test_private_or_unrelated_paths_never_reach_storage(self):
        for key in ('data/brain.json','data/portfolio.json','data/evidence/fred/../../private','audit-private/x'):
            with self.assertRaisesRegex(ValueError,'unapproved'):self.read(key)
        self.assertEqual(self.s.reads,[])

    def test_compiler_replacement_is_rejected(self):
        ref=store.retain(self.s,'b',self.inputs,self.output,self.histories);manifest=json.loads(self.read(ref['manifest_key']))
        self.s.objects[manifest['compilers']['plumbing_research_model']['key']]=b'altered'
        with self.assertRaisesRegex(ValueError,'reviewed compiler'):store.replay(ref,self.read)

    def test_whole_predecessor_is_preserved_and_publication_no_store(self):
        old=b'{"generated_at":"2026-09-19T00:00:00Z","score":99,"details":[1,2,3]}'
        self.s.objects[model.CURRENT]=old
        self.assertTrue(store.publish(self.s,'b',self.output))
        self.assertEqual(self.s.objects[store.PRIVATE+hashlib.sha256(old).hexdigest()+'.bin'],old)
        self.assertEqual(self.s.writes[-1]['CacheControl'],'no-store')

    def test_newer_compilation_cannot_roll_back_a_source_vintage(self):
        old=deepcopy(self.output);old['source_clocks']['macro']='2026-09-20T10:59:00Z'
        self.s.objects[model.CURRENT]=model.encoded(old)
        candidate={**self.output,'generated_at':'2026-09-20T11:30:00Z'}
        self.assertFalse(store.publish(self.s,'b',candidate));self.assertEqual(self.s.writes,[])

    def test_concurrent_newer_writer_wins_without_overwrite(self):
        self.s.objects[model.CURRENT]=model.encoded({'generated_at':'2026-09-19T00:00:00Z'})
        newer={**self.output,'generated_at':'2026-09-20T11:30:00Z'};put=self.s.put_object
        def race(**kw):
            if kw['Key']==model.CURRENT:self.s.objects[model.CURRENT]=model.encoded(newer)
            return put(**kw)
        with patch.object(self.s,'put_object',side_effect=race):self.assertFalse(store.publish(self.s,'b',self.output))
        self.assertEqual(json.loads(self.s.objects[model.CURRENT]),newer)

    def test_same_clock_conflict_is_not_silently_replaced(self):
        self.s.objects[model.CURRENT]=model.encoded({**self.output,'unexpected':True})
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(self.s,'b',self.output)
        self.assertEqual(self.s.writes,[])

    def test_real_public_store_retains_repo_context_without_score_or_private_reads(self):
        raw=b'{"generated_at":"2026-09-20T10:00:00Z","repo_stress_score":99}'
        self.s.objects['data/repo-market.json']=raw
        class Clock:
            @staticmethod
            def now(tz):return datetime(2026,9,20,11,tzinfo=timezone.utc)
        with patch.object(store,'datetime',Clock):result=store.run(self.s,'b')
        self.assertTrue(result['published']);packet=json.loads(self.s.objects[model.CURRENT])
        self.assertFalse(packet['context']['repo_market']['calls_eligible'])
        self.assertNotIn('repo_stress_score',packet['context']['repo_market'])
        self.assertEqual(self.s.objects[store.PRIVATE+hashlib.sha256(raw).hexdigest()+'.bin'],raw)
        self.assertTrue(all(store.allowed(k) or k.startswith(store.PRIVATE) for k in self.s.reads))

    def test_validation_only_never_writes_or_invokes(self):
        class Clock:
            @staticmethod
            def now(tz):return datetime(2026,9,20,11,tzinfo=timezone.utc)
        with patch.object(store,'datetime',Clock):result=store.run(self.s,'b',True)
        self.assertTrue(result['validation_only']);self.assertEqual(self.s.writes,[])


if __name__=='__main__':unittest.main()
