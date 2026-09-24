from pathlib import Path
from unittest.mock import Mock,patch
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import flow_state_store as s
import flow_state_model as m
from test_flow_state_model import fixture,AT
from test_option_flow_store import S3

def memory():
    inputs,blobs,docs=fixture();blobs.update({k:m.encoded(v) for k,v in docs.items()})
    return S3(blobs),inputs

class Tests(unittest.TestCase):
    def test_retention_and_replay_do_not_replace_a_current_packet(self):
        client,inputs=memory();before=client.data[m.CURRENT];read=s.reader(client,'bucket')
        out=m.compile_output(inputs,read);ref=s.retain(client,'bucket',inputs,out)
        self.assertEqual(s.replay(ref,read),out);self.assertEqual(client.data[m.CURRENT],before)
        self.assertTrue(all(row.get('IfNoneMatch')=='*' for row in client.writes))

    def test_completed_request_is_adopted_without_rewriting_or_recapturing(self):
        client,_=memory()
        with patch.object(s,'now',return_value=AT):result=s.run(client,'bucket','one-unique-request','aws-execution')
        self.assertTrue(result['published']);writes=len(client.writes)
        second=s.run(client,'bucket','one-unique-request')
        self.assertEqual(second['execution_id'],'aws-execution')
        self.assertTrue(second['adopted_completed_request']);self.assertEqual(len(client.writes),writes)
        self.assertEqual(m.strict(client.data[m.CURRENT])['replay'],result['replay'])
        public=[x for x in client.writes if x['Key']==m.CURRENT]
        self.assertEqual(len(public),1);self.assertIn('IfMatch',public[0]);self.assertEqual(public[0]['CacheControl'],'no-store')

    def test_failed_claim_stays_failed_and_cannot_recapture(self):
        client,_=memory();client.data['data/dark-pool.json']=b'null'
        with patch.object(s,'now',return_value=AT),self.assertRaises(ValueError):s.run(client,'bucket','failed-request')
        status=m.strict(client.data[s.request_key('failed-request')]);self.assertEqual(status['status'],'failed')
        count=len(client.writes)
        with self.assertRaisesRegex(ValueError,'incomplete'):s.run(client,'bucket','failed-request')
        self.assertEqual(len(client.writes),count)
        self.assertFalse(any(x['Key']==m.CURRENT for x in client.writes))

    def test_changed_predecessor_is_never_overwritten_or_retried(self):
        client,_=memory();base_put=client.put_object;replacement=m.encoded({'engine':'cross-asset-flow-state','version':'1.0','headline':'concurrent source change'})
        def put(**kw):
            if kw['Key']==m.CURRENT:client.data[m.CURRENT]=replacement
            return base_put(**kw)
        with patch.object(client,'put_object',side_effect=put),patch.object(s,'now',return_value=AT):result=s.run(client,'bucket','concurrent-request')
        self.assertFalse(result['published']);self.assertEqual(result['reason'],'predecessor_changed')
        self.assertEqual(client.data[m.CURRENT],replacement)

    def test_changed_compiler_or_retained_output_is_rejected(self):
        client,inputs=memory();read=s.reader(client,'bucket');out=m.compile_output(inputs,read);ref=s.retain(client,'bucket',inputs,out)
        run=s.verified_run(ref,read);client.data[run['output']['key']]+=b' '
        with self.assertRaises(ValueError):s.replay(ref,s.reader(client,'bucket'))
        client.data[run['output']['key']]=m.encoded(out);client.data[run['compilers']['flow_state_model']['key']]+=b'\n'
        with self.assertRaises(ValueError):s.replay(ref,s.reader(client,'bucket'))

    def test_private_accounts_mutable_heads_and_foreign_artifacts_rejected_before_read(self):
        client=Mock();read=s.reader(client,'bucket')
        for key in ('data/trade-tickets.json',m.CURRENT,'data/unknown/outputs/'+'a'*64+'.json',s.request_key('other')):
            with self.assertRaises(ValueError):read(key)
        client.get_object.assert_not_called()

    def test_parent_artifacts_cannot_be_written_by_composition(self):
        client=Mock();raw=b'{}';key='data/etf-research/outputs/'+m.sha(raw)+'.json'
        with self.assertRaises(ValueError):s.put_immutable(client,'bucket',key,raw)
        client.put_object.assert_not_called()

    def test_existing_conflicting_bytes_are_not_silently_replaced(self):
        raw=b'{}';key=m.PRIVATE+m.sha(raw)+'.bin';client=S3({key:b'conflict'})
        with self.assertRaises(ValueError):s.put_immutable(client,'bucket',key,raw)
        self.assertEqual(client.data[key],b'conflict')

    def test_new_generation_cannot_hide_parent_or_observation_rollback(self):
        client,inputs=memory();old=m.compile_output(inputs,s.reader(client,'bucket'))
        packet=copy.deepcopy(old);packet['generated_at']='2026-09-25T00:00:00Z'
        self.assertTrue(s.not_older(packet,old))
        for change in (lambda p:p['parents']['data/etf-true-flows.json'].update(generated_at='2026-09-23T20:00:00Z'),
            lambda p:p['foreign_flows'].update(observation_date='2026-06-01'),
            lambda p:p['asset_class_rotation'][0]['period'].update(end_date='2026-09-22')):
            bad=copy.deepcopy(packet);change(bad);self.assertFalse(s.not_older(bad,old))

if __name__=='__main__':unittest.main(verbosity=2)
