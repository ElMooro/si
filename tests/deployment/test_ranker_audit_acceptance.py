from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_ranker_audit_native_acceptance():
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_ranker_audit_acceptance.py')],cwd=R,check=True)
def test_ranker_audit_and_runner_boundaries():
 for name in ('test_ranker_audit_timing.py','test_ranker_runner_coverage.py'):subprocess.run([sys.executable,str(R/'tests'/name)],cwd=R,check=True)
