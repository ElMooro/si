from pathlib import Path
from copy import deepcopy
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6269_catalyst_context_acceptance as op


class Tests(unittest.TestCase):
    def fixture(self):
        p=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers'][op.FN]
        cfg=p['runtime'];mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        before={k:cfg[v] for k,v in mapping.items()}
        before.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=deepcopy(p['schedules']),source_files_checked=2,receipt={'status':'matched','commit':'synthetic'})
        return before,p
    def test_exact_package_does_not_claim_native_output_or_portfolio_acceptance(self):
        before,p=self.fixture();out=op.check(before,p,'synthetic')
        for field in ('native_output_verified','downstream_decisions_qualified','investment_authority'):self.assertIs(out[field],False)
    def test_wrong_package_partial_import_closure_or_schedule_fails(self):
        for edit in (lambda p:p['receipt'].update(commit='wrong'),lambda p:p.update(source_files_checked=1),lambda p:p.update(source_files_checked=6),lambda p:p.update(schedules=[]),lambda p:p.update(timeout=600)):
            before,p=self.fixture();edit(before)
            with self.assertRaises(ValueError):op.check(before,p,'synthetic')
    def test_operation_cannot_read_consumer_outputs_or_invoke_models(self):
        tree=ast.parse(Path(op.__file__).read_bytes())
        calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertFalse(calls & {'get_object','put_object','invoke','update_schedule','put_rule','publish','send_message','call_anthropic'})
        self.assertIn('exit',calls)


if __name__=='__main__':unittest.main(verbosity=2)
