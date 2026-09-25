from pathlib import Path
import ast, importlib.util, sys, unittest
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'aws/ops/staged/ops_6117_share_structure_part5_retained_verification.py'


class Tests(unittest.TestCase):
    def test_retained_only_part5_identity_and_no_reacquisition(self):
        spec=importlib.util.spec_from_file_location('share_part5_acceptance_test',PATH)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        self.assertEqual(module.REQUEST,'chatgpt-share-structure-part5-retained-6117')
        self.assertNotEqual(module.STATUS,module.source.request_key(module.batches.PARENT,'batch:5'))
        source=PATH.read_text(encoding='utf-8');tree=ast.parse(source)
        calls={node.func.attr for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)}
        self.assertTrue({'verify_batch','check','summarize','retain'}<=calls)
        self.assertFalse({'run_batch','invoke','get_parameter','get_secret_value','update_function_code','put_object'}&calls)
        self.assertIn("'batch:5'",source);self.assertIn("state['part'] == 5",source)
        self.assertIn('original_failed_run=36179729131',source)
        self.assertIn('ops_6087_share_structure_sources_part_5.md',source)
        self.assertIn('parent_control_unchanged',source)
        self.assertIn('sys.exit(1)',source)


if __name__=='__main__':unittest.main(verbosity=2)
