from pathlib import Path
import io, json, sys, unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/p) for p in ('aws/shared','aws/ops/checks')]
import statement_producer as producer
import statement_research_source as source
import statement_research_store_v2 as store
import statement_research_arithmetic_v2 as arithmetic
from test_statement_research_v2 import Fixture


class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
class Conflict(Exception):response={'Error':{'Code':'PreconditionFailed'}}


class S3:
    def __init__(self,files):self.files=dict(files);self.writes=[];self.race=False;self.ready_changed=False
    def etag(self,key):return '"'+source.sha(self.files[key])+'"'
    def get_object(self,Bucket,Key):
        if Key not in self.files:raise Missing()
        return {'Body':io.BytesIO(self.files[Key]),'ETag':self.etag(Key)}
    def head_object(self,Bucket,Key):
        return {'ETag':'changed' if self.ready_changed and Key==producer.READY else self.etag(Key)}
    def put_object(self,**request):
        key=request['Key']
        if request.get('IfNoneMatch')=='*' and key in self.files:raise Conflict()
        if request.get('IfMatch') and (self.race or request['IfMatch']!=self.etag(key)):raise Conflict()
        self.files[key]=request['Body'];self.writes.append(request)


def fixture():
    f=Fixture();s3=S3(f.files);compiled=f.compile();reference=store.retain(s3,'test',f.ref,f.identity_ref,compiled)
    proof=arithmetic.verify(f.ref,f.identity_ref,compiled,f.files.__getitem__)
    s3.files[producer.READY]=source.encoded({'contract':'financial-statement-qualified-ready.v2','status':'qualified',
        'source_manifest_sha256':f.ref['sha256'],'replay':reference,'qualification':proof})
    s3.files[producer.CURRENT]=source.encoded({'generated_at':'2026-02-28T12:00:00Z','legacy_fields':{'m_score':-1.1}})
    s3.writes=[]
    return f,s3,reference


def run(s3,request='unique-request',remaining=840):
    return producer.run(s3,'test',request,'actual-aws-execution',remaining,clock=lambda:'2026-03-01T13:00:00Z')


class Tests(unittest.TestCase):
    def test_replay_precedes_single_conditional_publication_and_whole_predecessor_is_retained(self):
        f,s3,ref=fixture();old=s3.files[producer.CURRENT];result=run(s3)
        self.assertTrue(result['published']);packet=json.loads(s3.files[producer.CURRENT])
        self.assertTrue(producer.current_matches(packet,ref));self.assertEqual(packet['provider_rows'],6)
        status=json.loads(s3.files[producer.request_key('unique-request')])
        self.assertEqual(s3.files[status['predecessor']['key']],old)
        publications=[v for v in s3.writes if v['Key']==producer.CURRENT]
        self.assertEqual(len(publications),1);self.assertIn('IfMatch',publications[0]);self.assertEqual(publications[0]['CacheControl'],'no-store')
        self.assertEqual(status['execution_id'],'actual-aws-execution')
        self.assertTrue(all(v['Key']==producer.CURRENT or v['Key'].startswith(source.PRIVATE) for v in s3.writes))

    def test_completed_request_is_idempotent_and_new_invocation_of_same_snapshot_is_noop(self):
        f,s3,ref=fixture();first=run(s3);writes=len(s3.writes)
        self.assertEqual(run(s3),first);self.assertEqual(len(s3.writes),writes)
        result=run(s3,'next-event');self.assertEqual(result['reason'],'unchanged_qualified_snapshot')
        self.assertEqual(sum(v['Key']==producer.CURRENT for v in s3.writes),1)

    def test_source_corruption_preserves_current_and_blocks_blind_retries(self):
        f,s3,ref=fixture();old=s3.files[producer.CURRENT]
        key=next(iter(f.capsules.values()))['original']['key'];s3.files[key]+=b' '
        with self.assertRaises(ValueError):run(s3)
        self.assertEqual(s3.files[producer.CURRENT],old)
        state=json.loads(s3.files[producer.request_key('unique-request')]);self.assertEqual(state['status'],'failed')
        self.assertEqual(s3.files[state['predecessor']['key']],old)
        with self.assertRaisesRegex(ValueError,'already attempted'):run(s3)

    def test_unqualified_ready_and_insufficient_budget_cannot_replace_head(self):
        for change in (lambda r:r.update(status='capturing'),lambda r:r['qualification'].update(metric_comparisons=0),
                lambda r:r['qualification'].update(forecast_qualified=True),lambda r:r.update(source_manifest_sha256='0'*64),
                lambda r:r['qualification'].update(current_sec_pairs_checked=False),
                lambda r:r['qualification'].update(identity_metadata_rows_checked=1)):
            f,s3,ref=fixture();old=s3.files[producer.CURRENT];r=json.loads(s3.files[producer.READY]);change(r);s3.files[producer.READY]=source.encoded(r)
            with self.assertRaises(ValueError):run(s3)
            self.assertEqual(s3.files[producer.CURRENT],old)
        f,s3,ref=fixture();old=s3.files[producer.CURRENT]
        with self.assertRaisesRegex(ValueError,'budget'):run(s3,remaining=100)
        self.assertEqual(s3.files[producer.CURRENT],old)

    def test_future_and_stale_acquisition_times_do_not_refresh_old_data(self):
        for clock in ('2026-02-28T13:00:00Z','2026-03-05T13:00:00Z'):
            f,s3,ref=fixture();old=s3.files[producer.CURRENT]
            with self.assertRaisesRegex(ValueError,'future-dated|48 hours'):
                producer.run(s3,'test','unique','actual',clock=lambda:clock)
            self.assertEqual(s3.files[producer.CURRENT],old)

    def test_concurrent_head_or_ready_changes_and_time_rollback_cannot_overwrite(self):
        for name,expected in (('race','concurrent_publication'),('ready_changed','new_ready_snapshot_available')):
            f,s3,ref=fixture();old=s3.files[producer.CURRENT];setattr(s3,name,True)
            self.assertEqual(run(s3)['reason'],expected);self.assertEqual(s3.files[producer.CURRENT],old)
        f,s3,ref=fixture();s3.files[producer.CURRENT]=source.encoded({'generated_at':'2026-03-01T12:30:00Z'})
        old=s3.files[producer.CURRENT];self.assertEqual(run(s3)['reason'],'observation_rollback_or_equal_time_conflict');self.assertEqual(s3.files[producer.CURRENT],old)

    def test_same_recorded_snapshot_can_repair_corrupted_current_bytes_with_retained_predecessor(self):
        f,s3,ref=fixture();run(s3);packet=json.loads(s3.files[producer.CURRENT]);packet['provider_rows']=1
        s3.files[producer.CURRENT]=source.encoded(packet);bad=s3.files[producer.CURRENT]
        result=run(s3,'repair-corrupted-head');self.assertTrue(result['published'])
        status=json.loads(s3.files[producer.request_key('repair-corrupted-head')]);self.assertEqual(s3.files[status['predecessor']['key']],bad)
        self.assertEqual(json.loads(s3.files[producer.CURRENT])['provider_rows'],6)


if __name__=='__main__':unittest.main(verbosity=2)
