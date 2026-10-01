"""Require complete confirmation-feed precision and native acceptance regressions."""
from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]
def test_complete_confirmation_numeric_projection_and_acceptance():
    for p in ('tests/test_confirmation_numeric_projection.py','aws/ops/checks/test_confirmation_numeric_acceptance.py'):
        subprocess.run([sys.executable,str(ROOT/p)],cwd=ROOT,check=True)
