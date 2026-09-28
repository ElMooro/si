from pathlib import Path
from copy import deepcopy
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6275_velocity_volume_acceptance as op


class Tests(unittest.TestCase):
    def fixture(self):
        original=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers'][op.FUNCTION]
        cfg=original['runtime'];mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        before={k:cfg[v] for k,v in mapping.items()}
        before.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=deepcopy(original['schedules']),source_files_checked=6,receipt={'status':'matched','commit':'synthetic'})
        return before,original
    def test_package_acceptance_does_not_qualify_consumer_outputs(self):
        before,p=self.fixture();out=op.check(before,p,'synthetic')
        for field in ('native_output_verified','consumer_decisions_qualified','investment_authority'):self.assertIs(out[field],False)
    def test_wrong_package_schedule_and_runtime_fail(self):
        for edit in (lambda p:p['receipt'].update(commit='wrong'),lambda p:p.update(source_files_checked=5),lambda p:p.update(schedules=[]),lambda p:p.update(timeout=999)):
            before,p=self.fixture();edit(before)
            with self.assertRaises(ValueError):op.check(before,p,'synthetic')
    def test_operation_has_no_consumer_output_reads_or_native_invocations(self):
        tree=ast.parse(Path(op.__file__).read_bytes());calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertFalse(calls & {'get_object','put_object','invoke','update_schedule','put_rule','publish','send_message'})
        self.assertIn('exit',calls)


if __name__=='__main__':unittest.main(verbosity=2)
