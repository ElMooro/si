from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]

def test_signal_logger_predecessor_packages():
    subprocess.run([sys.executable,str(R/'aws/ops/checks/test_signal_logger_packages.py')],cwd=R,check=True)
