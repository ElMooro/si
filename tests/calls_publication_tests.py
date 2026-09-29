"""Current publication identity, strict parsing and response lifecycle, offline."""
from pathlib import Path
from copy import deepcopy
import io,json,sys,unittest
from botocore.response import StreamingBody
from botocore.exceptions import IncompleteReadError
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import calls_research_replay as calls
AT='2026-09-29T18:00:00Z';OLD='2026-09-29T17:00:00Z';NEW='2026-09-29T19:00:00Z'
KEY='data/synthetic-publication.json'


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self,raw=None):self.raw=raw;self.version=0;self.bodies=[];self.writes=[];self.reads=0;self.before_put=None;self.fail_code=None
    def get_object(self,**kw):
        self.reads+=1
        if self.fail_code:raise StorageError(self.fail_code)
        if self.raw is None:raise StorageError('NoSuchKey')
        body=io.BytesIO(self.raw);self.bodies.append(body);return {'Body':body,'ETag':str(self.version)}
    def put_object(self,**kw):
        self.writes.append(kw)
        if self.before_put:self.before_put(self)
        if kw.get('IfNoneMatch')=='*' and self.raw is not None or kw.get('IfMatch',str(self.version))!=str(self.version):raise StorageError('PreconditionFailed')
        self.raw=kw['Body'];self.version+=1


def doc(at=AT,value=0):return {'generated_at':at,'value':value,'missing':None,'eligible':False}
def publish(s,value=None):return calls.publish_current(s,'synthetic',KEY,value if value is not None else doc())


class Publication(unittest.TestCase):
    def test_ambiguous_decoded_timestamps_and_nested_keys_fail_before_any_write(self):
        for raw in (b'{"generated_at":"2099-01-01T00:00:00Z","generated_at":"2020-01-01T00:00:00Z"}',
                    b'{"generated_at":"2020-01-01T00:00:00Z","generated_\\u0061t":"2020-01-01T00:00:00Z"}',
                    b'{"generated_at":"2020-01-01T00:00:00Z","nested":{"value":1,"value":2}}'):
            with self.subTest(raw=raw):
                s=Storage(raw)
                with self.assertRaises(ValueError):publish(s)
                self.assertEqual(s.raw,raw);self.assertEqual(s.writes,[]);self.assertTrue(all(b.closed for b in s.bodies))
    def test_nonfinite_constants_overflow_invalid_utf8_and_nonobjects_are_not_accepted(self):
        for raw in (b'{"x":NaN}',b'{"x":Infinity}',b'{"x":-Infinity}',b'{"x":1e999}',b'{"x":"\xff"}',b'[]',b'null',b'0'):
            with self.subTest(raw=raw):
                s=Storage(raw)
                with self.assertRaises((ValueError,TypeError)):publish(s)
                self.assertEqual(s.raw,raw);self.assertEqual(s.writes,[]);self.assertTrue(all(b.closed for b in s.bodies))
    def test_same_clock_changed_value_is_rejected_without_overwriting(self):
        raw=calls.canonical(doc(value=1));s=Storage(raw)
        with self.assertRaisesRegex(ValueError,'same-clock'):publish(s,doc(value=2))
        self.assertEqual(s.raw,raw);self.assertEqual(s.writes,[]);self.assertTrue(all(b.closed for b in s.bodies))
    def test_same_clock_numeric_zero_is_not_boolean_false_or_null(self):
        for value in (False,None,0.0):
            s=Storage(calls.canonical(doc(value=0)))
            with self.assertRaisesRegex(ValueError,'same-clock'):publish(s,doc(value=value))
            self.assertEqual(s.writes,[])
    def test_identical_same_clock_is_an_idempotent_no_write_without_renewing_storage(self):
        value=doc();raw=json.dumps(value,indent=2).encode();s=Storage(raw)
        self.assertEqual(publish(s,value),{'published':True,'reason':'already_current'})
        self.assertEqual(s.raw,raw);self.assertEqual(s.version,0);self.assertEqual(s.writes,[]);self.assertTrue(s.bodies[0].closed)
    def test_newer_current_is_preserved_and_response_is_closed(self):
        raw=calls.canonical(doc(NEW));s=Storage(raw)
        self.assertEqual(publish(s),{'published':False,'reason':'newer_current_present'})
        self.assertEqual(s.raw,raw);self.assertEqual(s.writes,[]);self.assertTrue(s.bodies[0].closed)
    def test_older_current_updates_conditionally_and_preserves_zero_false_null(self):
        s=Storage(calls.canonical(doc(OLD,99)));value=doc()
        self.assertEqual(publish(s,value),{'published':True});self.assertEqual(s.raw,calls.canonical(value))
        self.assertEqual(s.writes[0]['IfMatch'],'0');self.assertNotIn('IfNoneMatch',s.writes[0]);self.assertTrue(s.bodies[0].closed)
    def test_missing_current_creates_once_and_existing_undated_migration_stays_conditional(self):
        s=Storage();self.assertTrue(publish(s)['published']);self.assertEqual(s.writes[0]['IfNoneMatch'],'*')
        s=Storage(b'{"legacy":true}');self.assertTrue(publish(s)['published']);self.assertEqual(s.writes[0]['IfMatch'],'0');self.assertTrue(s.bodies[0].closed)
    def test_concurrent_newer_winner_is_preserved_after_conflict_and_retry(self):
        s=Storage(calls.canonical(doc(OLD)))
        def race(s):s.raw=calls.canonical(doc(NEW,5));s.version+=1;s.before_put=None
        s.before_put=race
        self.assertEqual(publish(s),{'published':False,'reason':'newer_current_present'})
        self.assertEqual(s.raw,calls.canonical(doc(NEW,5)));self.assertEqual(s.reads,2);self.assertTrue(all(b.closed for b in s.bodies))
    def test_transient_conflict_retries_same_document_and_conflict_exhaustion_is_fatal(self):
        raw=calls.canonical(doc(OLD));s=Storage(raw)
        def once(s):s.version+=1;s.before_put=None
        s.before_put=once;self.assertTrue(publish(s)['published']);self.assertEqual(s.reads,2);self.assertTrue(all(b.closed for b in s.bodies))
        s=Storage(raw);s.before_put=lambda s:setattr(s,'version',s.version+1)
        with self.assertRaises(RuntimeError):publish(s)
        self.assertEqual(s.raw,raw);self.assertEqual(s.reads,5);self.assertEqual(len(s.writes),5);self.assertTrue(all(b.closed for b in s.bodies))
    def test_sdk_incomplete_transfer_closes_without_writing_or_claiming_rollback(self):
        s=Storage(b'{}');underlying=io.BytesIO(b'{}');s.get_object=lambda **kw:{'Body':StreamingBody(underlying,100),'ETag':'0'}
        with self.assertRaises(IncompleteReadError):publish(s)
        self.assertTrue(underlying.closed);self.assertEqual(s.raw,b'{}');self.assertEqual(s.writes,[])
    def test_read_denial_and_invalid_candidate_never_create_or_replace_current(self):
        s=Storage(calls.canonical(doc(OLD)));s.fail_code='AccessDenied';before=s.raw
        with self.assertRaises(StorageError):publish(s)
        self.assertEqual(s.raw,before);self.assertEqual(s.writes,[])
        for value in ({'generated_at':AT,'value':float('nan')},{'generated_at':'2026-09-29'},None,[],0):
            s=Storage()
            with self.assertRaises((ValueError,TypeError)):calls.publish_current(s,'synthetic',KEY,value)
            self.assertEqual(s.reads,0);self.assertEqual(s.writes,[])


class Compatibility(unittest.TestCase):
    def test_both_reviewed_prior_runs_reproduce_using_current_code_without_mutation(self):
        for name in ('pre-liquidity-transport-calls-bundle.json','pre-calls-publication-bundle.json'):
            bundle=json.loads((ROOT/'tests/fixtures'/name).read_bytes());before=calls.canonical(bundle)
            result=calls.replay(bundle);self.assertEqual(result['compiler_match'],'reviewed_storage_transport_revision')
            self.assertEqual(result['output_sha256'],calls.digest(bundle['payload']['output']));self.assertEqual(calls.canonical(bundle),before)
            self.assertEqual(result['call_verb'],'WAIT');self.assertFalse(result['sizing_eligible'])
    def test_unknown_compiler_or_changed_conclusion_cannot_use_reviewed_compatibility(self):
        original=json.loads((ROOT/'tests/fixtures/pre-calls-publication-bundle.json').read_bytes())
        for kind in ('compiler','output'):
            bad=deepcopy(original)
            if kind=='compiler':bad['payload']['compiler']['files']['calls_research_replay.py']='a'*64
            else:bad['payload']['output']['brief_md']+=' changed conclusion'
            bad['payload_sha256']=calls.digest(bad['payload']);bad['run_id']='calls-research-'+bad['payload_sha256']
            with self.assertRaises(ValueError):calls.replay(bad)


def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(cls) for cls in (Publication,Compatibility)))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
