from pathlib import Path
import hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/ops/staged')]
import share_structure_batch_runner as runner
import ops_6103_share_structure_recovery_acceptance as acceptance


class Tests(unittest.TestCase):
    def test_whole_previous_runner_is_retained_and_frozen_sources_unchanged(self):
        manifest=json.loads((ROOT/'tests/fixtures/share-structure-completion.json').read_bytes())
        raw=(ROOT/manifest['predecessor']).read_bytes()
        self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(manifest['bytes'],manifest['sha256']))
        pinned=json.loads((ROOT/'tests/fixtures/share-structure-batch3-recovery.json').read_bytes())['frozen_collectors']
        for name,digest in pinned.items():self.assertEqual(hashlib.sha256((ROOT/('aws/ops/checks/'+name+'.py')).read_bytes()).hexdigest(),digest)

    def test_recovery_acceptance_requires_both_exact_whole_journals_and_manifest(self):
        state={'accepted_by_ops':'ops_6103_share_structure_recovery_acceptance',
            'recovered_from':acceptance.recovery.diagnostic.FAILED,'completed_recovery_journal':acceptance.COMPLETE,'manifest':acceptance.MANIFEST}
        self.assertEqual(runner.accepted_batch_report(3,state),'ops_6103_share_structure_recovery_acceptance.md')
        for field in ('recovered_from','completed_recovery_journal','manifest'):
            wrong={**state,field:{**state[field],'bytes':1}}
            with self.assertRaises(AssertionError):runner.accepted_batch_report(3,wrong)
        self.assertEqual(runner.accepted_batch_report(3,{}),'ops_6085_share_structure_sources_part_3.md')
        self.assertEqual(runner.accepted_batch_report(3,{'accepted_by_ops':'ops_6099_share_structure_batch3_recovery',
            'recovered_from':state['recovered_from']}),'ops_6099_share_structure_batch3_recovery.md')


if __name__=='__main__':unittest.main(verbosity=2)
