from pathlib import Path
import importlib.util,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/checks','aws/ops/staged','aws/ops')]
from test_statement_producer import fixture
import statement_research_store_v2 as store
spec=importlib.util.spec_from_file_location('scheduled_acceptance',ROOT/'aws/ops/staged/ops_6107_accounting_scheduled_snapshot_acceptance.py')
acceptance=importlib.util.module_from_spec(spec);spec.loader.exec_module(acceptance)


class Tests(unittest.TestCase):
    def test_warm_originals_use_real_frozen_compiler_and_independent_oracle_without_writes(self):
        f,s3,reference=fixture();read=store.reader(s3,'test');compiled=store.replay(reference,read)
        inputs={'source_manifest':f.ref,'identity_capture':f.identity_ref}
        before=dict(s3.files);proof=acceptance.verify_arithmetic(inputs,compiled,store.reader(s3,'test'))
        self.assertEqual(proof,f.verify(compiled));self.assertEqual(proof['metric_comparisons'],34)
        self.assertFalse(proof['production_measurement_formulas_imported']);self.assertEqual(s3.files,before);self.assertEqual(s3.writes,[])

    def test_preloading_does_not_make_changed_originals_or_wrong_arithmetic_acceptable(self):
        for kind in ('original','arithmetic'):
            f,s3,reference=fixture();compiled=store.replay(reference,store.reader(s3,'test'))
            if kind=='original':s3.files[next(iter(f.capsules.values()))['original']['key']]+=b' '
            else:compiled['shards']['ABC']['records'][0]['measurements']['metrics']['gross_margin_pct']['value']='999.000000000000'
            with self.assertRaises((ValueError,AssertionError)):
                acceptance.verify_arithmetic({'source_manifest':f.ref,'identity_capture':f.identity_ref},compiled,store.reader(s3,'test'))


if __name__=='__main__':unittest.main(verbosity=2)
