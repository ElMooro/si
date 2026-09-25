from pathlib import Path
from unittest.mock import Mock,patch
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests'),str(ROOT/'aws/shared')]
import ops_6033_gold_rotation_native_acceptance as op
from test_gold_rotation_store import memory,source_fixture,AT
from test_gold_rotation_native_handler import packet
EXECUTION='12345678-1234-1234-1234-123456789abc'
class Tests(unittest.TestCase):
    def test_one_request_has_a_durable_claim_and_an_actual_execution(self):
        client,inputs=memory();lam=Mock()
        def invoke(**request):
            self.assertEqual(request['FunctionName'],op.FUNCTION);self.assertEqual(request['InvocationType'],'Event')
            payload=json.loads(request['Payload'])
            with patch.object(op.store,'capture',side_effect=source_fixture(inputs)),patch.object(op.store,'now',return_value=AT):
                op.store.run(client,op.BUCKET,payload['request_id'],EXECUTION,'test-secret')
            return {'StatusCode':202}
        lam.invoke.side_effect=invoke;result=op.invoke(lam,client,'a'*40)
        self.assertTrue(result['invoke_sent']);self.assertTrue(result['status']['published']);self.assertEqual(result['status']['execution_id'],EXECUTION)
        with patch.object(op,'completed_request',return_value={**result,'invoke_sent':False}):self.assertFalse(op.invoke(lam,client,'a'*40)['invoke_sent'])
        self.assertEqual(lam.invoke.call_count,1)
    def test_ambiguous_dispatch_is_never_repeated(self):
        client,_=memory();lam=Mock();lam.invoke.side_effect=ConnectionError('ambiguous')
        with self.assertRaises(ConnectionError):op.invoke(lam,client,'b'*40)
        with self.assertRaisesRegex(AssertionError,'never blindly'):op.invoke(lam,client,'b'*40,wait_seconds=0)
        self.assertEqual(lam.invoke.call_count,1)
    def test_unrecognized_or_self_promoted_head_blocks_invocation(self):
        client,_=memory();client.data[op.model.CURRENT]=op.model.encoded({**packet(),'calls_eligible':True});lam=Mock()
        with self.assertRaises(AssertionError):op.invoke(lam,client,'c'*40)
        lam.invoke.assert_not_called()
    def test_observed_execution_must_fit_existing_runtime(self):
        logs=Mock();logs.filter_log_events.return_value={'events':[{'message':'report'}]};status={'execution_id':EXECUTION,'generated_at':AT}
        good={'memory_mb':512,'max_memory_mb':150,'duration_ms':20000}
        with patch.object(op,'parse_runtime',return_value=good):self.assertEqual(op.profile(logs,status)['execution_id'],EXECUTION)
        for bad in ({**good,'max_memory_mb':512},{**good,'duration_ms':180000},{**good,'status':'timeout'}):
            with patch.object(op,'parse_runtime',return_value=bad),self.assertRaises(AssertionError):op.profile(logs,status)
if __name__=='__main__':unittest.main(verbosity=2)
