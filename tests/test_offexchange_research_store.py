from pathlib import Path
from copy import deepcopy
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import offexchange_research_model as model
import offexchange_research_store as store
from test_offexchange_research_model import fixture
from test_option_flow_store import S3
class Tests(unittest.TestCase):
    def candidate(self):
        inputs,objects=fixture();client=S3(objects);compiled=model.compile_output(inputs,objects.__getitem__)
        ref=store.retain(client,'bucket',inputs,compiled);return client,inputs,compiled,ref
    def test_full_original_replay_reproduces_packet_and_every_shard_without_writes(self):
        client,inputs,compiled,ref=self.candidate();before=len(client.writes)
        self.assertEqual(store.replay(ref,store.reader(client,'bucket')),compiled);self.assertEqual(len(client.writes),before)
        self.assertNotIn(model.CURRENT,client.data);self.assertTrue(all(v['Key'].startswith(model.PREFIX) for v in client.writes))
    def test_repeated_identical_retention_is_idempotent_and_conflict_bytes_are_checked(self):
        client,inputs,compiled,ref=self.candidate();self.assertEqual(store.retain(client,'bucket',inputs,compiled),ref)
        client.data[ref['manifest_key']]+=b' '
        with self.assertRaisesRegex(ValueError,'readback differs'):store.retain(client,'bucket',inputs,compiled)
    def test_shard_tampering_or_missing_source_rejects_replay(self):
        client,inputs,compiled,ref=self.candidate();key=next(iter(compiled['packet']['record_shards'].values()))['key'];client.data[key]+=b' '
        with self.assertRaises(ValueError):store.replay(ref,store.reader(client,'bucket'))
        client,inputs,compiled,ref=self.candidate();del client.data[inputs['daily']['original']['key']]
        with self.assertRaises(Exception):store.replay(ref,store.reader(client,'bucket'))
    def test_invalid_shard_is_rejected_before_any_write(self):
        inputs,objects=fixture();client=S3(objects);compiled=model.compile_output(inputs,objects.__getitem__);compiled['packet']['record_shards'][model.bucket('AAPL')]['bytes']+=1
        with self.assertRaises(ValueError):store.retain(client,'bucket',inputs,compiled)
        self.assertEqual(client.writes,[])
    def test_current_keys_private_accounts_and_mutable_requests_are_out_of_scope(self):
        client=S3({})
        for key in (model.CURRENT,'data/trade-tickets.json',model.PRIVATE+'requests/'+('a'*64)+'.json'):
            with self.assertRaises(ValueError):store.reader(client,'bucket')(key)
            with self.assertRaises(ValueError):store.put_immutable(client,'bucket',key,b'{}')
        self.assertEqual(client.writes,[])
if __name__=='__main__':unittest.main(verbosity=2)
