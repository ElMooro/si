from pathlib import Path
from copy import deepcopy
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6273_positioning_observations_acceptance as op


class Tests(unittest.TestCase):
    def fixture(self,fn,count):
        original=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers'][fn]
        cfg=original['runtime'];mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        before={k:cfg[v] for k,v in mapping.items()}
        before.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=deepcopy(original['schedules']),source_files_checked=count,receipt={'status':'matched','commit':'synthetic'})
        return before,original
    def test_exact_packages_do_not_qualify_consumer_outputs(self):
        for fn,count in op.FUNCTIONS.items():
            before,baseline=self.fixture(fn,count);out=op.check(before,baseline,'synthetic',count)
            for flag in ('native_output_verified','consumer_decisions_qualified','investment_authority'):self.assertIs(out[flag],False)
    def test_incomplete_closure_wrong_commit_runtime_or_schedule_fails(self):
        for fn,count in op.FUNCTIONS.items():
            for change in (lambda p:p.update(source_files_checked=count-1),lambda p:p.update(schedules=[]),
                           lambda p:p['receipt'].update(commit='wrong'),lambda p:p.update(timeout=999)):
                before,baseline=self.fixture(fn,count);change(before)
                with self.assertRaises(ValueError):op.check(before,baseline,'synthetic',count)
    def test_acceptance_contains_no_consumer_output_or_native_calls(self):
        tree=ast.parse(Path(op.__file__).read_bytes());calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertFalse(calls & {'get_object','put_object','invoke','update_schedule','put_rule','publish','send_message'})
        self.assertIn('exit',calls)


if __name__=='__main__':unittest.main(verbosity=2)
