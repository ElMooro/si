from pathlib import Path
import hashlib,json,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/ops/staged','aws/ops')]
import share_structure_batch_runner as batches


class Tests(unittest.TestCase):
 def test_only_the_exact_accepted_journal_can_select_retained_acceptance(self):
  manifest={'key':batches.source.PRIVATE+'c003e6e2ea41ddd7fad66b08e08a3ba2c7eafb463ff788eb9a20a65a5eacd0ed.bin',
   'sha256':'c003e6e2ea41ddd7fad66b08e08a3ba2c7eafb463ff788eb9a20a65a5eacd0ed','bytes':616857}
  state={'status':'complete','part':4,'manifest':manifest,'captures':{}}
  self.assertEqual(batches.accepted_batch_report(4,state),'ops_6086_share_structure_sources_part_4.md')
  # Routing only: actual whole-journal hash and 2,000 originals were verified
  # by 6114. This mock never presents fabricated originals as source evidence.
  with patch.object(batches.source,'sha',return_value='5a85840604a7a2a0902c396adaef037bbec39ae9f600dab663a9be490bd3a895') as digest:
   self.assertEqual(batches.accepted_batch_report(4,state),'ops_6114_share_structure_part4_retained_verification.md')
   digest.assert_called_once_with(batches.source.encode(state))
   state['manifest']={**manifest,'bytes':1}
   with self.assertRaises(AssertionError):batches.accepted_batch_report(4,state)

 def test_whole_predecessor_and_all_frozen_collectors_are_preserved(self):
  migration=json.loads((ROOT/'tests/fixtures/share-structure-part4-acceptance.json').read_text())
  raw=(ROOT/migration['predecessor']).read_bytes()
  self.assertEqual(len(raw),migration['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),migration['sha256'])
  frozen=json.loads((ROOT/'tests/fixtures/share-structure-batch3-recovery.json').read_text())['frozen_collectors']
  for name,digest in frozen.items():
   path=ROOT/'aws/ops/checks'/str(name+'.py')
   self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),digest,name)
  self.assertEqual(batches.collection_limits(5),(2.0,5100))


if __name__=='__main__':unittest.main(verbosity=2)
