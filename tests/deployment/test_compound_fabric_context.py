from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_compound_fabric_context_whole_handler():
    subprocess.run([sys.executable,str(R/"aws/lambdas/justhodl-signal-fabric/tests/run_tests.py")],cwd=R,check=True)

def test_compound_overlays_native_acceptance():
    subprocess.run([sys.executable,str(R/"aws/ops/checks/test_compound_overlays_acceptance.py")],cwd=R,check=True)
