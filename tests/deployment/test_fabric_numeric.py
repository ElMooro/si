from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]

def test_fabric_numeric_and_real_consumer():
    subprocess.run([sys.executable,str(R/'aws/shared/tests/test_fabric_numeric.py')],cwd=R,check=True)

def test_fabric_numeric_native_acceptance():
    subprocess.run([sys.executable,str(R/"aws/ops/checks/test_fabric_numeric_acceptance.py")],cwd=R,check=True)
