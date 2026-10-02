from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_ranker_numeric_handler_and_invalid_inputs():
 subprocess.run([sys.executable,str(R/'tests/test_ranker_numeric.py')],cwd=R,check=True)
def test_ranker_numeric_native_acceptance():
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_ranker_numeric_acceptance.py')],cwd=R,check=True)
