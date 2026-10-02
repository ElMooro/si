"""Named native controls use invented clients, never actual feed bodies."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_heartbeat_controls():
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_heartbeat_controls.py')],cwd=R,check=True)
