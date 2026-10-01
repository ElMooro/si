"""Reject silent numeric corruption before releasing shared context importers."""
from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]
def test_context_numbers_consumers_and_exact_acceptance():
    for script in ('tests/test_context_numeric_projection.py','tests/test_context_numeric_consumers.py','aws/ops/checks/test_context_numeric_acceptance.py'):
        subprocess.run([sys.executable,str(ROOT/script)],cwd=ROOT,check=True)
