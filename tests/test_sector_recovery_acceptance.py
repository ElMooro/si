"""Run the same real dispatch-boundary tests against the recovery acceptance."""
from pathlib import Path
import ast,unittest,hashlib,types,tempfile
import test_sector_acceptance as shared
shared.PATH=Path(__file__).resolve().parents[1]/'aws/ops/staged/ops_5967_sector_native_recovery_acceptance.py'
SourceAcceptance=shared.SourceAcceptance
NativeAcceptance=shared.NativeAcceptance
class RecoveryIdentity(unittest.TestCase):
    def test_prior_failure_requires_the_committed_terminal_runtime_evidence(self):
        root=Path(__file__).resolve().parents[1]
        ns={'ROOT':root,'BASE_COMMIT':'cc78b3e809e7d9c7c3375db420dc26b2d078d2b8','model':types.SimpleNamespace(sha=lambda raw:hashlib.sha256(raw).hexdigest())}
        out=shared.actual('prior_failure',ns)();self.assertEqual(out['request']['execution_id'],'29b29ffe-ef65-4eff-9cf7-512a1fe565bb')
        with tempfile.TemporaryDirectory() as directory:
            copy=Path(directory)/out['diagnosis_report'];copy.parent.mkdir(parents=True)
            copy.write_text((root/out['diagnosis_report']).read_text(encoding='utf-8').replace("'status': 'timeout'","'status': 'unknown'"),encoding='utf-8')
            ns['ROOT']=Path(directory)
            with self.assertRaises(AssertionError):shared.actual('prior_failure',ns)()
    def test_only_changed_native_producers_require_new_receipts(self):
        ns={'FUNCTIONS':['justhodl-sector-rotation','justhodl-sector-tilt','justhodl-ai-brief'],'COMMIT':'new','BASE_COMMIT':'original'}
        expected=shared.actual('expected_commit',ns)
        self.assertEqual(expected('justhodl-sector-rotation'),'new');self.assertEqual(expected('justhodl-sector-tilt'),'new');self.assertEqual(expected('justhodl-ai-brief'),'original')
        with self.assertRaises(ValueError):expected('unreviewed')
if __name__=='__main__':unittest.main(verbosity=2)
