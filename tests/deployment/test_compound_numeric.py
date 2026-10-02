from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]

def test_compound_numeric_handler_and_invalid_inputs():
 subprocess.run([sys.executable,str(R/'aws/lambdas/justhodl-compound-aggregator/tests/run_tests.py')],cwd=R,check=True)

def test_compound_numeric_native_acceptance():
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_compound_numeric_acceptance.py')],cwd=R,check=True)
