from pathlib import Path
from unittest.mock import Mock,patch
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests'),str(ROOT/'aws/shared')]
import ops_6047_offexchange_alias_aware_acceptance as op
from test_offexchange_producer import Transport,FixedDateTime
from test_option_flow_store import S3
from test_offexchange_native_handler import packet
EXECUTION='12345678-1234-1234-1234-123456789abc'
class Tests(unittest.TestCase):
    def memory(self):return S3({op.model.CURRENT:op.model.encoded({'version':'2.6.0','generated_at':'2026-09-24T20:00:00Z'})})
    def test_one_durable_request_has_exact_execution_and_duplicate_is_not_sent(self):
        client=self.memory();lam=Mock()
        def invoke(**request):
            self.assertEqual(request['FunctionName'],op.FUNCTION);self.assertEqual(request['InvocationType'],'Event');payload=json.loads(request['Payload'])
            with patch.object(op.producer,'datetime',FixedDateTime):op.producer.run(client,op.BUCKET,payload['request_id'],EXECUTION,transport=Transport())
            return {'StatusCode':202}
        lam.invoke.side_effect=invoke;result=op.invoke(lam,client,'a'*40)
        self.assertTrue(result['invoke_sent']);self.assertTrue(result['status']['result']['published']);self.assertEqual(result['status']['execution_id'],EXECUTION)
        read=op.store.reader(client,op.BUCKET);run=op.store.verified_run(result['status']['result']['replay'],read);inputs=op.store.checked(run['input'],'inputs',read)
        self.assertTrue(op.selection_proof(inputs,read)['selected_partitions_equal_latest_advertised_periods'])
        inputs['partitions'].pop(next(iter(inputs['partitions'])))
        with self.assertRaises(AssertionError):op.selection_proof(inputs,read)
        with patch.object(op,'completed_request',return_value={**result,'invoke_sent':False}):self.assertFalse(op.invoke(lam,client,'a'*40)['invoke_sent'])
        self.assertEqual(lam.invoke.call_count,1)
    def test_ambiguous_dispatch_is_not_repeated(self):
        client=self.memory();lam=Mock();lam.invoke.side_effect=ConnectionError('ambiguous')
        with self.assertRaises(ConnectionError):op.invoke(lam,client,'b'*40)
        with self.assertRaisesRegex(AssertionError,'never blindly'):op.invoke(lam,client,'b'*40,wait_seconds=0)
        self.assertEqual(lam.invoke.call_count,1)
    def test_self_promoted_head_blocks_invocation(self):
        client=self.memory();client.data[op.model.CURRENT]=op.model.encoded({**packet(),'calls_eligible':True});lam=Mock()
        with self.assertRaises(AssertionError):op.invoke(lam,client,'c'*40)
        lam.invoke.assert_not_called()
    def test_completed_execution_must_fit_existing_allocation(self):
        logs=Mock();logs.filter_log_events.return_value={'events':[{'message':'report'}]};status={'execution_id':EXECUTION,'result':{'generated_at':'2026-09-25T03:00:00Z'}}
        good={'memory_mb':1024,'max_memory_mb':733,'duration_ms':140000}
        with patch.object(op,'parse_runtime',return_value=good):self.assertEqual(op.profile(logs,status)['execution_id'],EXECUTION)
        for bad in ({**good,'max_memory_mb':1024},{**good,'duration_ms':300000},{**good,'status':'timeout'}):
            with patch.object(op,'parse_runtime',return_value=bad),self.assertRaises(AssertionError):op.profile(logs,status)
if __name__=='__main__':unittest.main(verbosity=2)
