from pathlib import Path
from copy import deepcopy
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6268_cluster_abstention_acceptance as op


class Tests(unittest.TestCase):
    def fixture(self):
        p=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers'][op.FN]
        cfg=p['runtime'];mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        before={k:cfg[v] for k,v in mapping.items()}
        before.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=deepcopy(p['schedules']),source_files_checked=2,receipt={'status':'matched','commit':'synthetic-commit'})
        return before,p
    def test_exact_package_and_original_runtime_only_no_data_claim(self):
        before,p=self.fixture();v=op.check(before,p,'synthetic-commit')
        self.assertEqual(v['status'],'exact_package_and_original_schedule_verified')
        for k in ('native_output_verified','consumer_qualification','investment_authority'):self.assertIs(v[k],False)
    def test_wrong_receipt_partial_source_or_changed_cadence_fails(self):
        edits=[lambda p:p['receipt'].update(commit='other'),lambda p:p.update(source_files_checked=1),lambda p:p.update(memory_mb=2048),lambda p:p.update(schedules=[]),lambda p:p['schedules'][0].update(expression='rate(1 minute)')]
        for edit in edits:
            before,p=self.fixture();edit(before)
            with self.assertRaises(ValueError):op.check(before,p,'synthetic-commit')
    def test_operation_contains_no_output_reads_writes_invokes_or_paid_paths(self):
        tree=ast.parse(Path(op.__file__).read_bytes());calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertFalse(calls & {'invoke','get_object','put_object','update_schedule','put_rule','publish','send_message','call_anthropic'})
        self.assertIn('exit',calls)


if __name__=='__main__':unittest.main(verbosity=2)
