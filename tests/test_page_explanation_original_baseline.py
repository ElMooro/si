from pathlib import Path
import ast,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6216_page_explanation_original_baseline as op


class Tests(unittest.TestCase):
    def test_entire_original_sources_are_pinned_without_importing_the_producers(self):
        for fn in op.PINS:
            raw=(ROOT/'tests/fixtures'/('pre-explanation-research-'+fn.removeprefix('justhodl-')+'.py.txt')).read_bytes()
            self.assertEqual(op.source_check(fn,raw),{'bytes':len(raw),'sha256':op.PINS[fn],'imported_or_executed':False})
            with self.assertRaises(ValueError):op.source_check(fn,raw+b'\n')
            with self.assertRaises(ValueError):op.source_check('unrelated',raw)
    def test_missing_or_mismatched_actual_packages_cannot_be_reported_as_matched(self):
        with self.assertRaises(ValueError):op.package_summary({'status':'function_not_deployed'})
        row={'status':'whole_actual_package_retained','whole_zip':{'bytes':100},'repository_sources':{'private':'retained'},'inventory':{'code_matches_repository':False,'source_files_checked':1,'source_differences':['lambda_function.py']}}
        result=op.package_summary(row);self.assertFalse(result['code_matches_repository']);self.assertEqual(result['source_differences'],['lambda_function.py']);self.assertNotIn('repository_sources',result);self.assertNotIn('inventory',result)
    def test_operation_has_no_native_output_reads_invokes_or_configuration_writes(self):
        source=Path(op.__file__).read_text(encoding='utf-8');tree=ast.parse(source)
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call)]
        forbidden={'invoke','get_object','get_parameter','update_function_configuration','update_function_code','put_rule','update_schedule','delete_object','put_targets'}
        self.assertFalse(any(isinstance(n.func,ast.Attribute) and n.func.attr in forbidden for n in calls))
        self.assertEqual(set(op.PINS),{'justhodl-page-ai','justhodl-page-ai-commentary'})
        self.assertIn('sys.exit(1)',source);self.assertIn("row['code_matches_repository'] is True",source)


if __name__=='__main__':unittest.main(verbosity=2)
