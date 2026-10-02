from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_deterministic_desk_native_acceptance():
    subprocess.run([sys.executable,str(R/'aws/ops/checks/test_deterministic_desk_acceptance.py')],cwd=R,check=True)
