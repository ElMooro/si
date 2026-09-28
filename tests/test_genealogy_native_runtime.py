"""Whole orchestration, scratch teardown and source changes at publication."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import sys,tempfile,unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate','aws/ops/checks','aws/shared','tests')]
import genealogy_native_runtime as native
import genealogy_native_publication as publication
import genealogy_public_archive as archive
import genealogy_cache_checkpoint as checkpoint
import genealogy_revision_cache as revision
from test_genealogy_public_archive import fixture,NOW
from test_genealogy_revision_cache import Client
from test_genealogy_native_publication import DiskStore


class Runtime(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.originals,_,_=fixture();self.source=Client(self.originals)
        self.store=DiskStore(self.root/'store');self.cutoff=(NOW+timedelta(hours=1)).isoformat()
    def tearDown(self):self.tmp.cleanup()
    def run_native(self,name='run',cutoff=None,guard=None):
        return native.run(self.source,self.store,cutoff or self.cutoff,self.root/name,guard)

    def test_cold_and_whole_warm_runs_match_without_original_gets_or_old_scratch(self):
        cold=self.run_native('cold');self.assertTrue(cold['published'])
        self.assertEqual(cold['original_objects'],4);self.assertEqual(len(self.source.reads),4)
        self.assertFalse(list((self.root/'cold').glob('collect-*')))
        self.source.reads.clear();warm=self.run_native('warm')
        self.assertEqual(warm['reason'],'identical_head');self.assertEqual(cold['packet'],warm['packet'])
        self.assertEqual(self.source.reads,[]);self.assertEqual(warm['restored']['restored'],4)
        self.assertEqual(warm['cache']['misses'],0)
        manifest=archive.strict_json(publication.read_ref(self.store,warm['retained_manifest'],'runs'))
        self.assertEqual(len(manifest['compilers']),16)
        self.assertIn('genealogy_native_runtime',manifest['compilers'])
        self.assertFalse(self.store.path(checkpoint.CURRENT).exists())
        self.assertTrue(all(s.closed for s in self.source.streams+self.store.streams))

    def test_later_arrivals_and_page_boundaries_do_not_change_fixed_cutoff_identity(self):
        original=native.discover(self.source,self.cutoff)
        key=archive.PREFIX+'records/'+'f'*64+'.json'
        self.originals.objects[key]=(b'{}',NOW+timedelta(hours=2));self.source.tags[key]='"later"'
        def one_page(pages):return [{'Contents':[r for p in pages for r in p['Contents']],'IsTruncated':False}]
        self.source.mutate_pages=one_page
        self.assertEqual(native.discover(self.source,self.cutoff),original)
        self.assertEqual(revision.revisions(self.source,original[0]),original[1])

    def test_incomplete_duplicate_malformed_or_nonterminal_listings_cannot_write(self):
        mutations=[lambda p:p[:-1],lambda p:p+[p[-1]],
            lambda p:[{**v,'IsTruncated':True} for v in p],
            lambda p:[{**v,'IsTruncated':1} for v in p],
            lambda p:[{**p[0],'Contents':[p[0]['Contents'][0]]*2},*p[1:]],
            lambda p:[{**p[0],'Contents':{}}]]
        for n,mutation in enumerate(mutations):
            self.source.mutate_pages=mutation
            with self.subTest(n=n),self.assertRaises(ValueError):self.run_native('invalid'+str(n))
            self.assertEqual(self.store.writes,[])

    def test_revision_changes_after_retention_fail_after_replay_before_head(self):
        key=next(k for k in self.originals.objects if '/records/' in k)
        def mutate(kw):
            if '/runs/' in kw['Key']:
                self.source.tags[key]='"replacement"';self.store.before_put=lambda kw:None
        self.store.before_put=mutate
        with self.assertRaisesRegex(ValueError,'native_original_revisions_changed'):self.run_native()
        self.assertFalse(self.store.path(publication.CURRENT).exists())
        self.assertTrue((self.root/'run/replayed/head.json').exists())

    def test_generation_guard_aborts_before_head_and_cleans_first_calculation(self):
        first=self.run_native('initial');old=self.store.path(publication.CURRENT).read_bytes();phases=[]
        def guard(phase):
            phases.append(phase)
            if phase=='replay':raise RuntimeError('native_remaining_budget')
        with self.assertRaisesRegex(RuntimeError,'native_remaining_budget'):
            self.run_native('budget',(NOW+timedelta(hours=2)).isoformat(),guard)
        self.assertEqual(self.store.path(publication.CURRENT).read_bytes(),old)
        self.assertFalse(list((self.root/'budget').glob('collect-*')))
        self.assertEqual(phases,['inventory','restore','calculate','publication','retain','replay'])

    def test_corrupt_prior_original_archive_is_not_hidden_by_live_fallback(self):
        first=self.run_native('first');old=self.store.path(publication.CURRENT).read_bytes()
        ref=first['original_checkpoint'];path=self.store.path(ref['key']);path.write_bytes(path.read_bytes()+b' ')
        self.source.reads.clear()
        with self.assertRaisesRegex(ValueError,'checkpoint_reference_bytes'):
            self.run_native('corrupt',(NOW+timedelta(hours=2)).isoformat())
        self.assertEqual(self.source.reads,[]);self.assertEqual(self.store.path(publication.CURRENT).read_bytes(),old)

    def test_population_budget_refuses_truncation_before_any_write(self):
        with patch.object(checkpoint,'MAX_ENTRIES',3),self.assertRaisesRegex(ValueError,'population_exceeds_budget'):
            self.run_native()
        self.assertEqual(self.store.writes,[])

    def test_reopened_neutral_original_bytes_still_undergo_semantic_validation(self):
        self.run_native('first');old=self.store.path(publication.CURRENT).read_bytes()
        # Current validation must run even if every original is a warm cache hit.
        with patch.object(archive,'validate_capture',side_effect=ValueError('new_semantic_rule')):
            with self.assertRaises(ValueError):self.run_native('validation',(NOW+timedelta(hours=2)).isoformat())
        self.assertEqual(self.store.path(publication.CURRENT).read_bytes(),old)


if __name__=='__main__':unittest.main()
