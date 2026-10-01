"""Typed financial missingness and zero preservation gate."""
from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]
def test_financial_missing_zero_and_actual_consumers():
    for script in ('tests/test_financial_missing_zero.py','tests/test_financial_missing_zero_consumers.py','aws/ops/checks/test_financial_missing_zero_acceptance.py'):
        subprocess.run([sys.executable,str(ROOT/script)],cwd=ROOT,check=True)
