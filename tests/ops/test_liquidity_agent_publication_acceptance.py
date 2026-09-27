"""Publication acceptance must classify actual compilers, not a fixed status."""
from pathlib import Path
from unittest.mock import patch
import importlib.util,json,unittest

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('liquidity_acceptance',ROOT/'aws/ops/staged/ops_6152_liquidity_agent_binding_verify.py')
subject=importlib.util.module_from_spec(spec);spec.loader.exec_module(subject)


class Tests(unittest.TestCase):
    def check(self,old=False):
        packet={'generated_at':'2026-09-27T12:30:28+00:00','replay':{'manifest_key':'fixture-original'}}
        raw=(json.dumps(packet,indent=2)+'\n').encode()
        compilers={m.__name__:{'sha256':subject.store.sha(Path(m.__file__).read_bytes()),
                    'key':subject.store.model.PREFIX+'compilers/'+subject.store.sha(Path(m.__file__).read_bytes())+'.py'}
                   for m in subject.store.COMPILERS}
        if old:compilers['liquidity_agent_store']={'sha256':'old','key':'old'}
        proof={'replayed':True,'requested_series':73}
        reader=lambda key:raw
        with patch.object(subject.verifier,'verify',return_value=proof) as verify,patch.object(subject.store,'binding',return_value={'compilers':compilers}) as binding:
            actual,evidence=subject.publication(reader)
        verify.assert_called_once_with(packet,reader);binding.assert_called_once_with(packet,reader)
        self.assertEqual(actual,proof);self.assertEqual(evidence['bytes'],len(raw));self.assertEqual(evidence['sha256'],subject.store.sha(raw))
        self.assertEqual(evidence['current_compilers_match'],not old);self.assertEqual(evidence['compiler_count'],9)
        return evidence
    def test_current_package_is_identified_with_complete_exact_bytes(self):
        self.assertEqual(self.check()['status'],'current_compiler_publication_replayed')
    def test_reviewed_old_storage_never_claims_current_compilers(self):
        self.assertEqual(self.check(True)['status'],'reviewed_predecessor_storage_publication_replayed')
    def test_failed_or_incomplete_replay_cannot_be_accepted(self):
        for proof in ({'replayed':False,'requested_series':73},{'replayed':True,'requested_series':72}):
            with patch.object(subject.verifier,'verify',return_value=proof),patch.object(subject.store,'binding') as binding:
                with self.assertRaises(ValueError):subject.publication(lambda key:b'{}')
                binding.assert_not_called()
        with patch.object(subject.verifier,'verify',side_effect=ValueError('Complete replay differs')):
            with self.assertRaises(ValueError):subject.publication(lambda key:b'{}')


if __name__=='__main__':unittest.main()
