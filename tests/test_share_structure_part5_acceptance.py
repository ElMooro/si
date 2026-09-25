from pathlib import Path
import hashlib,json,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/ops/staged','aws/ops')]
import share_structure_batch_runner as batches


class Tests(unittest.TestCase):
 def test_only_the_exact_accepted_journal_can_select_retained_acceptance(self):
  manifest={'key':batches.source.PRIVATE+'cf316cf9205c3d147b0f043556a49f1a73feef5924e74bcc91315e97ea40ff2f.bin',
   'sha256':'cf316cf9205c3d147b0f043556a49f1a73feef5924e74bcc91315e97ea40ff2f','bytes':616939}
  state={'status':'complete','part':5,'manifest':manifest,'captures':{}}
  self.assertEqual(batches.accepted_batch_report(5,state),'ops_6087_share_structure_sources_part_5.md')
  # Routing only: actual whole-journal hash and 2,000 originals were verified
  # by 6117. This mock never presents fabricated originals as source evidence.
  with patch.object(batches.source,'sha',return_value='21fc7bb69f6a2e26b9c37be1be0dafad4babb485e305a7056777616842a86997') as digest:
   self.assertEqual(batches.accepted_batch_report(5,state),'ops_6117_share_structure_part5_retained_verification.md')
   digest.assert_called_once_with(batches.source.encode(state))
   state['manifest']={**manifest,'bytes':1}
   with self.assertRaises(AssertionError):batches.accepted_batch_report(5,state)

 def test_whole_predecessor_and_all_frozen_collectors_are_preserved(self):
  migration=json.loads((ROOT/'tests/fixtures/share-structure-part5-acceptance.json').read_text())
  raw=(ROOT/migration['predecessor']).read_bytes()
  self.assertEqual(len(raw),migration['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),migration['sha256'])
  frozen=json.loads((ROOT/'tests/fixtures/share-structure-batch3-recovery.json').read_text())['frozen_collectors']
  for name,digest in frozen.items():
   path=ROOT/'aws/ops/checks'/str(name+'.py')
   self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),digest,name)
  self.assertEqual(batches.collection_limits(6),(2.0,5100))


if __name__=='__main__':unittest.main(verbosity=2)
