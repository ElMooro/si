from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_compound_control_baseline_boundaries():
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_compound_controls.py')],cwd=R,check=True)
