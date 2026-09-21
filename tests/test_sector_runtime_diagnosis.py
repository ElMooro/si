import ast,unittest
from pathlib import Path
import re
PATH=Path(__file__).resolve().parents[1]/'aws/ops/staged/ops_5966_sector_tilt_runtime_diagnosis.py'
class RuntimeReports(unittest.TestCase):
    def test_only_managed_matching_execution_fields_are_emitted(self):
        tree=ast.parse(PATH.read_text(encoding='utf-8'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='parse_runtime');ns={'re':re}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(PATH),'exec'),ns);parse=ns['parse_runtime'];uid='a'*36
        result=parse('REPORT RequestId: '+uid+' Duration: 180000.00 ms Billed Duration: 180500 ms Memory Size: 256 MB Max Memory Used: 256 MB Status: error Error Type: Runtime.OutOfMemory extra-private-message',uid)
        self.assertEqual(result,{'duration_ms':180000.0,'billed_duration_ms':180500.0,'memory_mb':256.0,'max_memory_mb':256.0,'status':'error','error_type':'Runtime.OutOfMemory'})
        self.assertIsNone(parse('user log '+uid+' private message',uid));self.assertIsNone(parse('REPORT RequestId: another-request Duration: 10 ms',uid))
        self.assertNotIn('private',str(result))
if __name__=='__main__':unittest.main(verbosity=2)
