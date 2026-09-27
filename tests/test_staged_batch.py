"""Actual temporary Git indexes; no remote, credentials, commit or push in validator."""
from pathlib import Path
from tempfile import TemporaryDirectory
import importlib.util
import subprocess
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('staged_batch',ROOT/'scripts/check_staged_batch.py')
batch=importlib.util.module_from_spec(spec);spec.loader.exec_module(batch)


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory(prefix='reviewed-staged-batch-');self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.git('init','-q');self.git('config','user.email','fixture@example.invalid');self.git('config','user.name','Fixture')
        self.write('engine.py',b'original\n');self.git('add','engine.py');self.git('commit','-qm','original')
    def git(self,*args):return subprocess.check_output(['git',*args],cwd=self.root,stderr=subprocess.DEVNULL)
    def write(self,name,raw):
        path=self.root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)

    def test_complete_related_large_file_tree_matches_commit_without_write_cap(self):
        self.write('engine.py',b'# complete large source\n'*4000);self.write('helper.py',b'helper\n')
        self.git('add','engine.py','helper.py');before=self.git('status','--porcelain=v1')
        proof=batch.check(self.root,['engine.py','helper.py'])
        self.assertEqual(proof['status'],'complete_staged_inventory');self.assertFalse(proof['deployment_verified'])
        self.assertEqual(self.git('status','--porcelain=v1'),before)
        self.git('commit','-qm','whole batch');self.assertEqual(self.git('rev-parse','HEAD^{tree}').decode().strip(),proof['staged_tree'])

    def test_restored_but_unstaged_tracked_member_is_rejected(self):
        self.write('engine.py',b'new handler\n');self.write('helper.py',b'helper\n')
        self.git('add','helper.py')
        with self.assertRaisesRegex(batch.BatchError,'missing_from_index.*engine.py'):batch.check(self.root,['engine.py','helper.py'])
        # Also reject an accidentally incomplete declared list: worktree is not the tested index.
        with self.assertRaisesRegex(batch.BatchError,'unstaged_tracked.*engine.py'):batch.check(self.root,['helper.py'])

    def test_staged_file_edited_after_validation_cannot_silently_commit_older_bytes(self):
        self.write('engine.py',b'first edit\n');self.git('add','engine.py');self.write('engine.py',b'final tested edit\n')
        with self.assertRaisesRegex(batch.BatchError,'unstaged_tracked'):batch.check(self.root,['engine.py'])

    def test_missing_new_member_and_unexpected_staged_member_fail(self):
        self.write('engine.py',b'new\n');self.write('helper.py',b'new\n');self.git('add','engine.py')
        with self.assertRaisesRegex(batch.BatchError,'missing_from_index'):batch.check(self.root,['engine.py','helper.py'])
        self.git('add','helper.py')
        with self.assertRaisesRegex(batch.BatchError,'unexpected_staged'):batch.check(self.root,['engine.py'])

    def test_renames_deletions_and_spaces_have_explicit_inventory(self):
        self.git('mv','engine.py','engine name.py')
        proof=batch.check(self.root,['engine.py','engine name.py'])
        self.assertEqual(set(proof['files']),{'engine.py','engine name.py'})

    def test_malformed_duplicate_absolute_and_traversal_inventory_fail(self):
        for value in ([],{},['engine.py','engine.py'],['../outside'],['/absolute'],['C:/outside'],['./engine.py'],['a\\b'],[None]):
            with self.subTest(value=value),self.assertRaises(batch.BatchError):batch.check(self.root,value)


if __name__=='__main__':unittest.main(verbosity=2)
