from pathlib import Path
from unittest.mock import Mock,patch
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests'),str(ROOT/'aws/shared')]
import ops_6030_flow_state_native_acceptance as op
from test_flow_state_store import memory
from test_flow_state_native_handler import packet
EXECUTION='12345678-1234-1234-1234-123456789abc'

class Tests(unittest.TestCase):
    def test_one_native_request_uses_durable_claim_and_actual_execution(self):
        client,_=memory();lam=Mock()
        def invoke(**request):
            self.assertEqual(request['FunctionName'],op.FUNCTION);self.assertEqual(request['InvocationType'],'Event')
            payload=json.loads(request['Payload']);op.store.run(client,op.BUCKET,payload['request_id'],EXECUTION)
            return {'StatusCode':202}
        lam.invoke.side_effect=invoke
        result=op.invoke(lam,client,'a'*40)
        self.assertTrue(result['invoke_sent']);self.assertTrue(result['status']['published']);self.assertEqual(result['status']['execution_id'],EXECUTION)
        self.assertIn(result['dispatch_key'],client.data);self.assertEqual(lam.invoke.call_count,1)
        with patch.object(op,'completed_request',return_value={**result,'invoke_sent':False}):
            self.assertFalse(op.invoke(lam,client,'a'*40)['invoke_sent'])
        self.assertEqual(lam.invoke.call_count,1)

    def test_ambiguous_transport_failure_leaves_claim_and_never_reinvokes(self):
        client,_=memory();lam=Mock();lam.invoke.side_effect=ConnectionError('ambiguous')
        with self.assertRaises(ConnectionError):op.invoke(lam,client,'b'*40)
        key=op.store.request_key('chatgpt-flow-state-native-'+('b'*12)+'-1-dispatch')
        self.assertEqual(json.loads(client.data[key])['status'],'claimed')
        with self.assertRaisesRegex(AssertionError,'never blindly'):op.invoke(lam,client,'b'*40,wait_seconds=0)
        self.assertEqual(lam.invoke.call_count,1)

    def test_unknown_or_self_promoted_head_prevents_dispatch(self):
        client,_=memory();client.data[op.model.CURRENT]=op.model.encoded({**packet(),'calls_eligible':True});lam=Mock()
        with self.assertRaises(AssertionError):op.invoke(lam,client,'c'*40)
        lam.invoke.assert_not_called()

    def test_completed_execution_must_fit_existing_memory_and_timeout(self):
        logs=Mock();logs.filter_log_events.return_value={'events':[{'message':'report'}]}
        status={'execution_id':EXECUTION,'generated_at':'2026-09-24T23:00:00Z'}
        good={'memory_mb':256,'max_memory_mb':100,'duration_ms':6000}
        with patch.object(op,'parse_runtime',return_value=good):self.assertEqual(op.profile(logs,status)['execution_id'],EXECUTION)
        for bad in ({**good,'max_memory_mb':256},{**good,'duration_ms':60000},{**good,'status':'timeout'}):
            with patch.object(op,'parse_runtime',return_value=bad),self.assertRaises(AssertionError):op.profile(logs,status)

if __name__=='__main__':unittest.main(verbosity=2)
