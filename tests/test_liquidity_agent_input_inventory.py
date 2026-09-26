from pathlib import Path
import hashlib,io,sys,unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
from liquidity_agent_input_inventory import inventory
SOURCE=ROOT/'aws/lambdas/justhodl-liquidity-agent/source/lambda_function.py'


class Tests(unittest.TestCase):
    def test_literal_non_catalog_input_and_original_cache_are_included(self):
        out=inventory(SOURCE.read_bytes())
        self.assertEqual(len(out['catalog_series']),72);self.assertEqual(len(out['all_series']),73)
        self.assertEqual(out['additional_literal_series'],['SP500'])
        self.assertEqual(out['unresolved_requests'],[])
        self.assertEqual(out['s3_inputs'],['data/china-liquidity.json','data/fred-cache.json',
                                         'data/global-liquidity.json','liquidity-data.json'])
        self.assertFalse(out['legacy_labels_units_and_categories_qualified'])

    def test_adding_a_hidden_reader_changes_inventory_instead_of_dropping_it(self):
        raw=SOURCE.read_bytes()+b'\ndef extra():\n    return get_series_history("UNRATE")\n'
        self.assertEqual(inventory(raw)['additional_literal_series'],['SP500','UNRATE'])
        raw+=b'\ndef dynamic(settings):\n    return get_latest(settings["series"])\n'
        self.assertEqual(inventory(raw)['unresolved_requests'][0]['function'],'dynamic')

    def test_no_predecessor_code_is_executed_and_duplicate_catalog_is_rejected(self):
        raw=SOURCE.read_bytes()+b'\nraise AssertionError("must not execute")\n'
        self.assertEqual(len(inventory(raw)['all_series']),73)
        with self.assertRaises(ValueError):inventory(b'FRED_SERIES=[("WALCL","a","u","w"),("WALCL","b","u","w")]')

    def test_scope_completion_reads_only_complete_bound_private_predecessors(self):
        sys.path.insert(0,str(ROOT/'aws/ops/staged'))
        import ops_6124_liquidity_agent_scope_completion as operation
        raw=b'complete predecessor\r\n';digest=hashlib.sha256(raw).hexdigest()
        ref={'key':operation.baseline.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        client=Mock();client.get_object.return_value={'Body':io.BytesIO(raw)}
        self.assertEqual(operation.read_original(client,ref),raw)
        client.reset_mock()
        with self.assertRaises(ValueError):operation.read_original(client,dict(ref,key='data/portfolio.json'))
        client.get_object.assert_not_called()
        client.get_object.return_value={'Body':io.BytesIO(raw[:-1])}
        with self.assertRaises(ValueError):operation.read_original(client,ref)
        text=Path(operation.__file__).read_text(encoding='utf-8')
        for forbidden in ('.invoke(','.update_function_code(','.update_function_configuration(','.put_rule(','.urlopen('):
            self.assertNotIn(forbidden,text)
        self.assertIn('sys.exit(1)',text)


if __name__=='__main__':unittest.main(verbosity=2)
