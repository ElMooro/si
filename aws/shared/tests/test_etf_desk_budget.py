"""Offline adversarial execution admission; no transport hard-deadline claim."""
from pathlib import Path
from unittest import mock
from types import SimpleNamespace
from concurrent.futures import Future
import io,json,sys,threading,unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).parent)]
import etf_desk_store as store
import test_etf_desk_store as original_tests
from test_etf_desk_store import fixture,AT
from test_etf_holdings_store import Storage


class BudgetUnit(unittest.TestCase):
    def test_short_budget_admits_no_io(self):
        for remaining in (-1,0,1,120,360):
            db=Storage()
            with self.subTest(remaining=remaining),self.assertRaises(store.BudgetExceeded):
                store.run(db,'fixture','short','execution',remaining_seconds=remaining)
            self.assertEqual((db.reads,db.writes),([],[]))

    def test_chunk_deadline_closes_body_and_latches_abort(self):
        clock=[0];body=io.BytesIO(b'x'*200000)
        class Slow:
            def read1(self,n):clock[0]+=400;return body.read(n)
            def close(self):body.close()
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]):
            budget=store.ExecutionBudget(900)
            with self.assertRaises(store.BudgetExceeded):store.BudgetBody(Slow(),budget).read(200000)
            self.assertTrue(body.closed);self.assertTrue(budget.stopped)

    def test_get_returning_after_deadline_closes_unconsumed_body(self):
        clock=[0];body=io.BytesIO(b'{}')
        def get(**kw):clock[0]=1001;return {'Body':body}
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]):
            budget=store.ExecutionBudget(900);client=store.BudgetClient(SimpleNamespace(get_object=get),budget)
            with self.assertRaises(store.BudgetExceeded):client.get_object(Key='test')
            self.assertTrue(body.closed)

    def test_sdk_retry_guard_rejects_late_attempt_and_oversized_config(self):
        from botocore.config import Config
        from botocore.hooks import HierarchicalEmitter
        events=HierarchicalEmitter();clock=[0]
        client=SimpleNamespace(meta=SimpleNamespace(config=Config(connect_timeout=5,read_timeout=20,
            retries={'total_max_attempts':3,'mode':'legacy'}),events=events))
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]):
            budget=store.ExecutionBudget(900);state=store.bind_retry_guard(client,budget)
            events.emit('before-send.s3.PutObject');clock[0]=650
            with self.assertRaises(store.BudgetExceeded):events.emit('before-send.s3.PutObject')
            state.checkpoint=True;events.emit('before-send.s3.PutObject')
            state.checkpoint=False
            with self.assertRaises(store.BudgetExceeded):events.emit('before-send.s3.PutObject')
        client.meta.config=Config(connect_timeout=5,read_timeout=20,retries={'total_max_attempts':4})
        with self.assertRaises(ValueError):store.bind_retry_guard(client,store.ExecutionBudget(900))

    def test_real_sdk_never_exceeds_three_attempts_offline(self):
        import boto3
        from botocore.config import Config
        from botocore.exceptions import ConnectTimeoutError
        client=boto3.client('s3',region_name='us-east-1',aws_access_key_id='synthetic',aws_secret_access_key='synthetic',
                            config=Config(connect_timeout=5,read_timeout=20,retries={'total_max_attempts':3,'mode':'legacy'}))
        store.bind_retry_guard(client,store.ExecutionBudget(900))
        with mock.patch.object(client._endpoint.http_session,'send',side_effect=ConnectTimeoutError(endpoint_url='https://offline.invalid')) as send, mock.patch('botocore.endpoint.time.sleep'):
            with self.assertRaises(ConnectTimeoutError):client.get_object(Bucket='fixture',Key='test')
        self.assertEqual(send.call_count,3)

    def test_budgetless_writer_still_surfaces_readback_failure(self):
        read=store.reader(Storage(),'fixture')
        with mock.patch.object(store.ArtifactWriter,'write',side_effect=ValueError('readback')):
            with self.assertRaisesRegex(ValueError,'readback'):
                with store.ArtifactWriter(Storage(),'fixture',read) as emit:emit('key',b'body')

    def test_active_io_survives_cancel_but_no_followup_or_queued_write(self):
        started=threading.Event();release=threading.Event();finished=threading.Event();db=Storage()
        budget=store.ExecutionBudget(900);budget.DRAIN=0.02
        original=db.put_object
        def slow(**kw):
            started.set();release.wait(2);original(**kw);finished.set()
        db.put_object=slow;client=store.BudgetClient(db,budget);read=store.reader(client,'fixture')
        writer=store.ArtifactWriter(client,'fixture',read,workers=1,pending_limit=2)
        raw=b'{}';key=store.PRIVATE+store.model.sha(raw)+'.bin'
        writer(key,raw);self.assertTrue(started.wait(1));writer(key,raw)
        try:
            with self.assertRaises(store.BudgetExceeded):writer.__exit__(ValueError,ValueError(),None)
            self.assertFalse(finished.is_set());self.assertTrue(budget.stopped)
        finally:release.set()
        self.assertTrue(finished.wait(1));writer.pool.shutdown(wait=True)
        self.assertEqual(db.writes,[key]);self.assertEqual(db.reads,[])
        self.assertNotIn(store.model.CURRENT,db.objects)

    def test_pagination_checks_each_page(self):
        clock=[0];calls=[]
        def listing(**kw):calls.append(kw);clock[0]+=400;return {'IsTruncated':True,'NextContinuationToken':'next'}
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]):
            client=store.BudgetClient(SimpleNamespace(list_objects_v2=listing),store.ExecutionBudget(900))
            with self.assertRaises(store.BudgetExceeded):list(client.get_paginator('list_objects_v2').paginate(Bucket='fixture'))
        self.assertEqual(len(calls),2)


class BudgetRun(unittest.TestCase):
    setUp=original_tests.RetainedDesk.setUp
    # Inherit existing complete replay/identity/recovery regressions under the guard.
    def payload(self,inputs):
        return {k:inputs[k] for k in ('query_date','profiles','extra_flows','extra_holdings','provider_requests','original_provider_bytes')}

    def test_reviewed_main_v1_v2_reconstruct_exact_bytes(self):
        import gzip
        cases=json.loads(gzip.decompress((Path(__file__).resolve().parents[3]/'tests/fixtures/desk-budget-predecessor-synthetic.json.gz').read_bytes()))
        for case in cases:
            db=Storage();db.objects={k:v.encode() for k,v in case['objects'].items()};packet=case['packet']
            with self.subTest(version=case['version']):
                self.assertEqual(store.model.encoded(store.replay(packet['replay'],store.reader(db,'fixture'))),
                                 store.model.encoded({k:v for k,v in packet.items() if k!='replay'}))

    def test_replay_failure_keeps_durable_candidate_and_old_aliases(self):
        db,inputs=fixture();aliases={key:db.objects[key] for key in store.ALIASES}
        with mock.patch.object(store,'collect',return_value=self.payload(inputs)),mock.patch.object(store,'replay',side_effect=ValueError('replay failed')):
            with self.assertRaises(RuntimeError):store.run(db,'fixture','bad-replay','execution')
        self.assertNotIn(store.model.CURRENT,db.objects)
        self.assertEqual({key:db.objects[key] for key in aliases},aliases)
        status=json.loads(db.objects[store.request_key('bad-replay')]);self.assertEqual(status['status'],'failed')
        self.assertIn(status['candidate_replay']['manifest_key'],db.objects)

    def test_collection_overrun_preserves_public_and_admits_no_compile(self):
        db,inputs=fixture();before=dict(db.objects);clock=[0]
        def overrun(*args):clock[0]=1001;return self.payload(inputs)
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(store,'collect',side_effect=overrun),mock.patch.object(store,'compile_output') as compile_:
            with self.assertRaises(RuntimeError):store.run(db,'fixture','overrun','execution')
            compile_.assert_not_called()
        self.assertNotIn(store.model.CURRENT,db.objects)
        for key in store.ALIASES:self.assertEqual(db.objects[key],before[key])
        # No unsafe last-gasp write once all reserve was lost.
        self.assertEqual(json.loads(db.objects[store.request_key('overrun')])['status'],'running')

    def test_compiler_cpu_overrun_withholds_retention_and_records_failure(self):
        db,inputs=fixture();clock=[0];real=store.model.build
        def build(*args):out=real(*args);clock[0]=700;return out
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(store,'collect',return_value=self.payload(inputs)),mock.patch.object(store.model,'build',side_effect=build):
            with self.assertRaises(RuntimeError):store.run(db,'fixture','compile-overrun','execution')
        self.assertNotIn(store.model.CURRENT,db.objects)
        status=json.loads(db.objects[store.request_key('compile-overrun')]);self.assertEqual(status['status'],'failed')
        self.assertIn('retained_input',status)

    def test_failure_before_root_preserves_previous_and_complete_replay(self):
        db,inputs=fixture();aliases={key:db.objects[key] for key in store.ALIASES};clock=[0]
        original=store.retain
        def retain(*args):ref=original(*args);clock[0]=550;return ref
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(store,'collect',return_value=self.payload(inputs)),mock.patch.object(store,'now',return_value=AT),mock.patch.object(store,'retain',side_effect=retain):
            with self.assertRaises(RuntimeError):store.run(db,'fixture','before-root','execution')
        self.assertNotIn(store.model.CURRENT,db.objects)
        self.assertEqual({key:db.objects[key] for key in aliases},aliases)
        status=json.loads(db.objects[store.request_key('before-root')]);self.assertEqual(status['status'],'failed')
        store.replay(status['candidate_replay'],store.reader(db,'fixture'))

    def test_root_accepted_response_lost_has_replay_without_blind_rollback(self):
        db,inputs=fixture();aliases={key:db.objects[key] for key in store.ALIASES}
        def accepted_lost(client,bucket,key,raw,condition):
            client.put_object(Bucket=bucket,Key=key,Body=raw,**condition)
            raise OSError('response lost after commit')
        with mock.patch.object(store,'collect',return_value=self.payload(inputs)),mock.patch.object(store,'now',return_value=AT):
            with self.assertRaises(RuntimeError):store.run(db,'fixture','lost','execution',publish=accepted_lost)
        status=json.loads(db.objects[store.request_key('lost')]);packet=json.loads(db.objects[store.model.CURRENT])
        self.assertEqual(packet['replay'],status['candidate_replay'])
        self.assertEqual({key:db.objects[key] for key in aliases},aliases)
        self.assertEqual(store.model.encoded(store.replay(packet['replay'],store.reader(db,'fixture'))),store.model.encoded({k:v for k,v in packet.items() if k!='replay'}))

    def test_late_root_return_cannot_start_aliases(self):
        db,inputs=fixture();aliases={key:db.objects[key] for key in store.ALIASES};clock=[0];put=db.put_object
        def late(**kw):
            put(**kw)
            if kw['Key']==store.model.CURRENT:clock[0]=1001
        db.put_object=late
        with mock.patch.object(store.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(store,'collect',return_value=self.payload(inputs)),mock.patch.object(store,'now',return_value=AT):
            with self.assertRaises(RuntimeError):store.run(db,'fixture','late','execution')
        self.assertEqual({key:db.objects[key] for key in aliases},aliases)
        status=json.loads(db.objects[store.request_key('late')]);packet=json.loads(db.objects[store.model.CURRENT])
        self.assertEqual(packet['replay'],status['candidate_replay'])

if __name__=='__main__':unittest.main()
