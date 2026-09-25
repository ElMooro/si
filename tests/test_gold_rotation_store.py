from pathlib import Path
from unittest.mock import Mock,patch
from io import BytesIO
import copy,sys,time,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from test_gold_rotation_model import fixture,AT
from test_option_flow_store import S3
import gold_rotation_model as m
import gold_rotation_store as s

def memory():
    inputs,blobs,_=fixture();blobs[m.CURRENT]=blobs[inputs['predecessor']['key']]
    return S3(blobs),inputs
def source_fixture(inputs):
    def capture(client,bucket,symbol,kind,start,end,credential,deadline,transport):
        assert (start,end)==('2024-04-08','2026-09-24');return copy.deepcopy(inputs['captures'][symbol+':'+kind])
    return capture
class Tests(unittest.TestCase):
    def test_native_collector_retains_whole_bytes_and_keeps_auth_out_of_references(self):
        client,_=memory();raw=b'[{"symbol":"GLD","date":"2026-09-24","price":391.69}]'
        response=BytesIO(raw);response.status=200;transport=Mock(return_value=response)
        with patch.object(s,'now',return_value=AT):ref=s.capture(client,'bucket','GLD','light','2024-04-08','2026-09-24','test-secret',time.monotonic()+20,transport)
        self.assertEqual(client.data[ref['original']['key']],raw);self.assertNotIn('test-secret',m.encoded(ref).decode())
        request=transport.call_args.args[0];self.assertEqual(request.get_header('Apikey'),'test-secret');self.assertNotIn('test-secret',request.full_url)
        self.assertEqual(transport.call_count,1)
    def test_native_collector_refuses_credential_echo_and_unapproved_symbol_before_storage(self):
        client,_=memory();response=BytesIO(b'{"secret":"test-secret"}');response.status=200;transport=Mock(return_value=response)
        with self.assertRaises(ValueError):s.capture(client,'bucket','GLD','light','2024-04-08','2026-09-24','test-secret',time.monotonic()+20,transport)
        self.assertEqual(client.writes,[])
        with self.assertRaises(ValueError):s.capture(client,'bucket','ACCOUNT','light','2024-04-08','2026-09-24','test-secret',time.monotonic()+20,transport)
        self.assertEqual(transport.call_count,1)
    def test_retention_replays_originals_without_changing_the_current_packet(self):
        client,inputs=memory();before=client.data[m.CURRENT];read=s.reader(client,'bucket')
        out=m.compile_output(inputs,read);ref=s.retain(client,'bucket',inputs,out)
        self.assertEqual(s.replay(ref,read),out);self.assertEqual(client.data[m.CURRENT],before)
        self.assertTrue(all(r['IfNoneMatch']=='*' for r in client.writes))
    def test_one_conditional_native_write_and_adopted_request_never_recollects(self):
        client,inputs=memory()
        with patch.object(s,'capture',side_effect=source_fixture(inputs)) as capture,patch.object(s,'now',return_value=AT):
            result=s.run(client,'bucket','once','actual-aws-request','secret');self.assertEqual(capture.call_count,32)
            second=s.run(client,'bucket','once','another-aws-request','secret');self.assertEqual(capture.call_count,32)
        self.assertTrue(result['published']);self.assertEqual(second['execution_id'],'actual-aws-request');self.assertTrue(second['adopted_completed_request'])
        heads=[r for r in client.writes if r['Key']==m.CURRENT];self.assertEqual(len(heads),1);self.assertIn('IfMatch',heads[0]);self.assertEqual(heads[0]['CacheControl'],'no-store')
    def test_failed_capture_cannot_be_silently_retried(self):
        client,inputs=memory()
        with patch.object(s,'capture',side_effect=ValueError('source issue')),patch.object(s,'now',return_value=AT),self.assertRaises(ValueError):s.run(client,'bucket','failed','execution','secret')
        self.assertEqual(m.strict(client.data[s.request_key('failed')])['status'],'failed')
        with patch.object(s,'capture',side_effect=AssertionError('No recapture')),self.assertRaisesRegex(ValueError,'incomplete'):s.run(client,'bucket','failed','execution','secret')
        self.assertFalse(any(r['Key']==m.CURRENT for r in client.writes))
    def test_a_changed_predecessor_is_never_rebased_or_overwritten(self):
        client,inputs=memory();put=client.put_object;replacement=m.encoded({'engine':'gold-equity-rotation','as_of':AT,'other':'publisher'})
        def race(**kw):
            if kw['Key']==m.CURRENT:client.data[m.CURRENT]=replacement
            return put(**kw)
        with patch.object(s,'capture',side_effect=source_fixture(inputs)),patch.object(s,'now',return_value=AT),patch.object(client,'put_object',side_effect=race):result=s.run(client,'bucket','race','execution','secret')
        self.assertFalse(result['published']);self.assertEqual(result['reason'],'predecessor_changed');self.assertEqual(client.data[m.CURRENT],replacement)
    def test_changed_output_or_compiler_bytes_fail_replay(self):
        client,inputs=memory();read=s.reader(client,'bucket');out=m.compile_output(inputs,read);ref=s.retain(client,'bucket',inputs,out);run=s.verified_run(ref,read)
        for key in (run['output']['key'],run['compilers']['gold_rotation_model']['key']):
            saved=client.data[key];client.data[key]+=b' '
            with self.assertRaises(ValueError):s.replay(ref,s.reader(client,'bucket'))
            client.data[key]=saved
    def test_reader_rejects_accounts_and_foreign_paths_before_any_read(self):
        client=Mock();read=s.reader(client,'bucket')
        for key in ('data/trade-tickets.json',m.CURRENT,'data/etf-research/outputs/'+'a'*64+'.json',m.PREFIX+'outputs/'+'a'*64+'.py'):
            with self.assertRaises(ValueError):read(key)
        client.get_object.assert_not_called()
    def test_later_generation_cannot_hide_observation_rollback_or_missing_prices(self):
        client,inputs=memory();old=m.compile_output(inputs,s.reader(client,'bucket'));new=copy.deepcopy(old);new['generated_at']='2026-09-26T00:40:30Z'
        self.assertTrue(s.not_older(new,old))
        for change in (lambda p:p['instruments']['GLD'].update(observation_date='2026-09-23'),lambda p:p['instruments']['GLD']['latest'].update(full=None)):
            bad=copy.deepcopy(new);change(bad);self.assertFalse(s.not_older(bad,old))

if __name__=='__main__':unittest.main(verbosity=2)
