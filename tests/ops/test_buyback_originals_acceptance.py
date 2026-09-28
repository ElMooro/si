from pathlib import Path
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6291_buyback_originals_acceptance as op
spec=importlib.util.spec_from_file_location('buyback_originals_native',ROOT/'aws/lambdas/justhodl-buyback-engine/tests/test_measurements.py')
n=importlib.util.module_from_spec(spec);spec.loader.exec_module(n)


class Tests(unittest.TestCase):
    def test_actual_native_whole_source_history_and_measurement_replay(self):
        ns,db,_,_,_=n.captured_handler();ns['lambda_handler']();result=op.publication(db.raw[op.KEY],db)
        self.assertTrue(result['publication_history_verified']);self.assertEqual(result['provider_replay']['retained_responses'],11)
        self.assertEqual(result['source_files_verified'],4);self.assertFalse(result['investment_authority'])
        self.assertTrue(all(k==op.KEY or k.startswith((op.store.PREFIX,op.sources.PREFIX)) for k in db.reads))

    def test_old_native_publication_remains_pending_without_history_reads(self):
        db=n.Memory();result=op.publication(db.raw[op.KEY],db)
        self.assertFalse(result['publication_history_verified']);self.assertEqual(db.reads,[])

    def test_source_compiler_original_and_archive_tampering_fails(self):
        ns,db,_,_,_=n.captured_handler();ns['lambda_handler']();raw=db.raw[op.KEY];packet=json.loads(raw)
        ref=packet['provider_sources']['attempts'][0]['original_ref'];db.raw[ref['key']]=b'{}'
        with self.assertRaises(ValueError):op.publication(raw,db)
        packet['source_files'].pop('buyback_sources.py');body=json.dumps(packet).encode()
        with self.assertRaises(ValueError):op.publication(body,db)
        packet=json.loads(raw);packet.pop('publication_contract')
        with self.assertRaises(ValueError):op.publication(json.dumps(packet).encode(),db)
        db.raw[op.store.reference(raw)['key']]=b'{}'
        with self.assertRaises(ValueError):op.publication(raw,db)

    def test_out_of_scope_history_is_rejected_before_storage_read(self):
        db=n.Memory()
        for ref in ({'key':'data/private.json','bytes':2,'sha256':'0'*64},
                    {'key':op.store.PREFIX+'0'*64+'.json','bytes':True,'sha256':'0'*64}):
            with self.assertRaises(ValueError):op.retained(db,ref)
        self.assertEqual(db.reads,[])


if __name__=='__main__':unittest.main(verbosity=2)
