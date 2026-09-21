"""Read-only diagnosis counts retained originals without treating it as replay."""
from pathlib import Path
from collections import Counter
from unittest import mock
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import etf_holdings_model as model
import etf_holdings_store as store
from test_etf_holdings_store import fixture
PATH=ROOT/'aws/ops/staged/ops_5979_etf_holdings_runtime_diagnosis.py'


class Diagnosis(unittest.TestCase):
    def test_counts_whole_distinct_sources_and_large_snapshot_components(self):
        db,inputs=fixture()
        with mock.patch.dict(model.catalog.ETF_UNIVERSE,{'SPY':{}},clear=True):
            out=store.compile_output(inputs,store.reader(db,'fixture'),lambda k,b:store.immutable(db,'fixture',k,b))
        snapshots=[json.loads(db.objects[f[role]['snapshot']['key']]) for f in out['funds'].values() for role in ('current','prior')]
        node=next(n for n in ast.parse(PATH.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='inventory')
        ns={'Counter':Counter,'model':model};exec(compile(ast.Module(body=[node],type_ignores=[]),str(PATH),'exec'),ns)
        got=ns['inventory'](inputs,out,snapshots)
        self.assertEqual(got['configured_funds'],1);self.assertEqual(got['normalized_rows_current_and_prior'],3)
        self.assertEqual(got['snapshot_count'],2);self.assertEqual(got['row_artifacts'],2)
        self.assertEqual(got['collections'],{'complete_returned_snapshot':2})
        self.assertGreater(got['distinct_provider_original_bytes'],0)
        self.assertLess(got['distinct_predecessor_originals'],got['predecessor_contexts'])
        with self.assertRaises(AssertionError):ns['inventory'](inputs,out,snapshots[:1])
    def test_diagnosis_cannot_invoke_or_mutate_cloud_state(self):
        tree=ast.parse(PATH.read_text());calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertTrue({'get_object','get_function_configuration','filter_log_events'}<=calls)
        self.assertFalse(calls & {'invoke','put_object','delete_object','update_function_code','update_function_configuration','put_rule','update_schedule'})
        source=PATH.read_text();self.assertIn('raw_log_messages_returned=0',source)
        self.assertIn("'full_original_replay_this_diagnosis':False",source)


if __name__=='__main__':unittest.main(verbosity=2)
