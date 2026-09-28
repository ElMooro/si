from pathlib import Path
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6289_backlog_publication_acceptance as op
spec=importlib.util.spec_from_file_location('backlog_publication_native',ROOT/'aws/lambdas/justhodl-backlog/tests/run_tests.py');n=importlib.util.module_from_spec(spec);spec.loader.exec_module(n)


class Tests(unittest.TestCase):
    def test_whole_actual_publication_prior_and_arithmetic_replay(self):
        ns,_,_=n.Tests().handler();db=ns['s3'];prior=db.raw[op.KEY];ns['lambda_handler']();raw=db.raw[op.KEY]
        result=op.publication(raw,prior);self.assertTrue(result['publication_history_verified']);self.assertFalse(result['investment_authority'])
        self.assertEqual(op.retained(db,op.store.reference(raw)),raw);self.assertEqual(op.retained(db,op.store.reference(prior)),prior)
    def test_wrong_compiler_prior_archive_and_active_measurements_fail(self):
        ns,_,_=n.Tests().handler();db=ns['s3'];prior=db.raw[op.KEY];ns['lambda_handler']();raw=db.raw[op.KEY]
        for edit in (lambda p:p['source_files'].pop('backlog_store.py'),lambda p:p.update(previous_publication=None),lambda p:p['by_ticker']['TEST'].update(rpo_qoq=999)):
            p=json.loads(raw);edit(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode(),prior)
        ref=op.store.reference(raw);db.raw[ref['key']]=b'{}'
        with self.assertRaises(ValueError):op.retained(db,ref)
    def test_out_of_scope_history_is_rejected_before_storage_read(self):
        ns,_,_=n.Tests().handler();db=ns['s3'];before=list(db.reads)
        for ref in ({'key':'data/private.json','sha256':'0'*64,'bytes':2},{'key':op.store.PREFIX+'0'*64+'.json','sha256':'0'*64,'bytes':True}):
            with self.assertRaises(ValueError):op.retained(db,ref)
        self.assertEqual(db.reads,before)
    def test_earlier_native_version_stays_pending(self):
        self.assertFalse(op.publication(b'{"version":"1.1.0"}')['publication_history_verified'])


if __name__=='__main__':unittest.main(verbosity=2)
