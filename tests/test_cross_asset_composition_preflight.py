from pathlib import Path
from unittest.mock import Mock
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6017_cross_asset_composition_preflight as op
from test_futures_research_store import Memory
class Tests(unittest.TestCase):
    def test_whole_packet_bytes_and_unknown_fields_are_retained(self):
        s=Memory();key=op.NATIVE['fx'][0];raw=b'{ "contract":"original", "new_field":[0,null] }\n';s.data[key]=raw
        ref=op.capture(s,key,op.store.reader(s,'bucket'));self.assertEqual(s.data[ref['original']['key']],raw)
    def test_private_or_foreign_capture_is_rejected_before_read(self):
        s=Mock()
        for key in ('data/trade-tickets.json','portfolio/current.json','https://example.com/a'):
            with self.assertRaises(AssertionError):op.capture(s,key,lambda k:b'')
        s.get_object.assert_not_called()
    def test_inventory_does_not_convert_generation_to_observation_or_replay(self):
        result=op.describe(op.NATIVE['fx'][0],{'generated_at':'2026-09-21T00:00:00Z','pairs':{'USD_JPY':{}},'replay':{'unverified':True}})
        self.assertIsNone(result['source_capture_completed_at']);self.assertFalse(result['original_provider_replay_performed_by_this_inventory'])
    def test_acceptance_must_bind_exact_output_and_authority(self):
        packet={'contract':'test','replay':{'identity':'exact'},**op.model.PERMISSIONS}
        proof={'publication':{'replay':packet['replay'],'sha256':op.model.sha(op.model.encoded(packet))},'original_source_replay_matches':True,'originals_anonymously_denied':True}
        op.proof_matches(packet,proof)
        for change in (lambda p:p['publication'].update(sha256='0'*64),lambda p:p.update(original_source_replay_matches=False),lambda p:p['publication'].update(replay={'wrong':True})):
            bad=copy.deepcopy(proof);change(bad)
            with self.assertRaises(AssertionError):op.proof_matches(packet,bad)
        with self.assertRaises(ValueError):op.proof_matches({**packet,'sizing_eligible':True},proof)
if __name__=='__main__':unittest.main(verbosity=2)
