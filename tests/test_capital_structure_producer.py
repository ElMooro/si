"""Actual compiler/oracle/replay publication boundary with synthetic originals."""
from pathlib import Path
from unittest.mock import patch
import io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/checks','tests')]
import capital_structure_producer as producer
import capital_structure_source as source
import capital_structure_store as store
import capital_structure_arithmetic as arithmetic
from test_capital_structure_research import fixture as source_fixture,compile_fixture


class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
class Conflict(Exception):response={'Error':{'Code':'PreconditionFailed'}}


class S3:
    def __init__(self,files):
        self.files=dict(files);self.writes=[];self.race=False;self.ready_changed=False
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
    f=source_fixture();s3=S3(f['files']);compiled=compile_fixture(f)
    reference=store.retain(s3,'test',f['manifest'],f['identity'],compiled)
    proof=arithmetic.verify(f['manifest'],f['identity'],compiled,f['files'].__getitem__)
    s3.files[producer.READY]=source.encoded({'contract':producer.READY_CONTRACT,'status':'qualified',
        'source_manifest_sha256':f['manifest']['sha256'],'replay':reference,'qualification':proof,
        'counts':{key:compiled['packet'][key] for key in ('reported_names','provider_responses','provider_rows','empty_responses')}})
    s3.files[producer.CURRENT]=source.encoded({'generated_at':'2026-09-24T12:00:00Z',
        'tickers':{'ABC':{'dilution_rate':99,'score':100}},'entire_legacy_payload':['retained',{'without':'truncation'}]})
    s3.writes=[]
    return f,s3,reference


def run(client,request='unique',remaining=900,clock='2026-09-25T15:00:00Z'):
    return producer.run(client,'test',request,'actual-aws-execution',remaining,clock=lambda:clock)


class Tests(unittest.TestCase):
    def test_actual_original_replay_precedes_conditional_publication_and_whole_predecessor_survives(self):
        f,s3,reference=fixture();old=s3.files[producer.CURRENT];result=run(s3)
        self.assertTrue(result['published']);packet=json.loads(s3.files[producer.CURRENT])
        self.assertTrue(producer.current_matches(packet,reference));self.assertEqual(packet['provider_rows'],7)
        self.assertEqual(packet['generated_at'],'2026-09-25T11:01:01Z')
        status=json.loads(s3.files[producer.request_key('unique')])
        self.assertEqual(s3.files[status['predecessor']['key']],old)
        self.assertEqual(status['execution_id'],'actual-aws-execution')
        writes=[v for v in s3.writes if v['Key']==producer.CURRENT]
        self.assertEqual(len(writes),1);self.assertIn('IfMatch',writes[0]);self.assertEqual(writes[0]['CacheControl'],'no-store')
        self.assertTrue(all(v['Key']==producer.CURRENT or v['Key'].startswith(source.PRIVATE) for v in s3.writes))
        for key in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):self.assertIs(packet[key],False)

    def test_idempotent_request_and_same_snapshot_never_write_a_second_head(self):
        f,s3,reference=fixture();first=run(s3);writes=len(s3.writes)
        self.assertEqual(run(s3),first);self.assertEqual(len(s3.writes),writes)
        self.assertEqual(run(s3,'another-event')['reason'],'unchanged_qualified_snapshot')
        self.assertEqual(sum(v['Key']==producer.CURRENT for v in s3.writes),1)

    def test_bad_original_record_or_compiler_cannot_publish_or_silently_retry(self):
        for kind in ('original','issuer','compiler'):
            f,s3,reference=fixture();old=s3.files[producer.CURRENT]
            if kind=='original':key=next(iter(f['originals'].values()))['key']
            else:
                run_doc=json.loads(s3.files[reference['manifest_key']])
                key=(next(iter(run_doc['compilers'].values()))['key'] if kind=='compiler' else
                    json.loads(s3.files[run_doc['output']['key']])['issuers'][0]['record']['key'])
            s3.files[key]+=b' '
            with self.assertRaises(ValueError):run(s3)
            self.assertEqual(s3.files[producer.CURRENT],old)
            state=json.loads(s3.files[producer.request_key('unique')]);self.assertEqual(state['status'],'failed')
            with self.assertRaisesRegex(ValueError,'already attempted'):run(s3)

    def test_missing_qualification_or_population_mismatch_preserves_current(self):
        changes=(lambda r:r.update(status='capturing'),lambda r:r.update(qualification=None),
            lambda r:r['qualification'].update(metric_comparisons=0),
            lambda r:r['qualification'].update(identity_metadata_rows_checked=1),
            lambda r:r['qualification'].update(current_sec_pairs_checked=False),
            lambda r:r['qualification'].update(production_measurement_formulas_imported=True),
            lambda r:r['qualification'].update(forecast_qualified=True),
            lambda r:r['counts'].update(reported_names=2),lambda r:r.update(source_manifest_sha256='0'*64))
        for change in changes:
            f,s3,reference=fixture();old=s3.files[producer.CURRENT]
            ready=json.loads(s3.files[producer.READY]);change(ready);s3.files[producer.READY]=source.encoded(ready)
            with self.assertRaises(ValueError):run(s3)
            self.assertEqual(s3.files[producer.CURRENT],old)

    def test_future_or_stale_acquisition_and_exhausted_runtime_never_refresh_old_data(self):
        for clock in ('2026-09-24T15:00:00Z','2026-09-28T15:00:00Z'):
            f,s3,reference=fixture();old=s3.files[producer.CURRENT]
            with self.assertRaisesRegex(ValueError,'future-dated|48 hours'):run(s3,clock=clock)
            self.assertEqual(s3.files[producer.CURRENT],old)
        for remaining in (100,299.9):
            f,s3,reference=fixture();old=s3.files[producer.CURRENT]
            with self.assertRaisesRegex(ValueError,'budget'):run(s3,remaining=remaining)
            self.assertEqual(s3.files[producer.CURRENT],old)
        f,s3,reference=fixture();old=s3.files[producer.CURRENT];replay=store.replay;tick=[0]
        def expire_after_replay(*args):
            result=replay(*args);tick[0]=1000;return result
        with patch.object(producer.time,'monotonic',side_effect=lambda:tick[0]),patch.object(store,'replay',side_effect=expire_after_replay):
            with self.assertRaisesRegex(TimeoutError,'budget'):run(s3)
        self.assertEqual(s3.files[producer.CURRENT],old)

    def test_concurrent_ready_or_head_and_rollback_cannot_overwrite(self):
        for attribute,reason in (('race','concurrent_publication'),('ready_changed','new_ready_snapshot_available')):
            f,s3,reference=fixture();old=s3.files[producer.CURRENT];setattr(s3,attribute,True)
            self.assertEqual(run(s3)['reason'],reason);self.assertEqual(s3.files[producer.CURRENT],old)
        for stamp in ('2026-09-25T11:01:01Z','2026-09-25T12:00:00Z'):
            f,s3,reference=fixture();s3.files[producer.CURRENT]=source.encoded({'generated_at':stamp})
            old=s3.files[producer.CURRENT]
            self.assertEqual(run(s3)['reason'],'observation_rollback_or_equal_time_conflict')
            self.assertEqual(s3.files[producer.CURRENT],old)

    def test_same_snapshot_repairs_tampered_head_and_preserves_the_tampered_predecessor(self):
        f,s3,reference=fixture();run(s3);packet=json.loads(s3.files[producer.CURRENT]);packet['provider_rows']=1
        s3.files[producer.CURRENT]=source.encoded(packet);old=s3.files[producer.CURRENT]
        self.assertTrue(run(s3,'repair')['published'])
        state=json.loads(s3.files[producer.request_key('repair')])
        self.assertEqual(s3.files[state['predecessor']['key']],old)
        self.assertEqual(json.loads(s3.files[producer.CURRENT])['provider_rows'],7)


if __name__=='__main__':unittest.main(verbosity=2)
