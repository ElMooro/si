from pathlib import Path
import copy,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6290_backlog_originals_acceptance as op
spec=importlib.util.spec_from_file_location('backlog_originals_native',ROOT/'aws/lambdas/justhodl-backlog/tests/run_tests.py')
n=importlib.util.module_from_spec(spec);spec.loader.exec_module(n)


class Tests(unittest.TestCase):
    def test_actual_native_history_source_identity_and_originals_replay(self):
        ns,db,_,_=n.captured_handler();ns['lambda_handler']()
        result=op.publication(db.raw[op.KEY],db)
        self.assertTrue(result['publication_history_verified'])
        self.assertTrue(result['provider_replay']['whole_provider_http_replay_verified'])
        self.assertEqual(result['source_files_verified'],4)
        self.assertFalse(result['provider_replay']['selection_context_replayed'])
        self.assertTrue(all(k==op.KEY or k.startswith((op.store.PREFIX,op.sources.PREFIX)) for k in db.reads))

    def test_prior_native_is_pending_without_archive_or_private_reads(self):
        db=n.Memory({'by_ticker':{}})
        result=op.publication(b'{"version":"1.1.0"}',db)
        self.assertEqual(result['status'],'pending_original_schedule_source_capture');self.assertEqual(db.reads,[])

    def test_tampered_whole_original_archive_and_compiler_cannot_pass(self):
        ns,db,_,_=n.captured_handler();ns['lambda_handler']();raw=db.raw[op.KEY];packet=json.loads(raw)
        ref=packet['provider_sources']['attempts'][0]['original_ref'];db.raw[ref['key']]=b'{}'
        with self.assertRaises(ValueError):op.publication(raw,db)
        altered=copy.deepcopy(packet);altered['source_files'].pop('backlog_sources.py')
        body=json.dumps(altered).encode();db.raw[op.store.reference(body)['key']]=body
        with self.assertRaises(ValueError):op.publication(body,db)


if __name__=='__main__':unittest.main(verbosity=2)
