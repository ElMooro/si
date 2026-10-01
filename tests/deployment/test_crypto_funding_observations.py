"""Funding units, capture/replay and consumer regressions use invented data only."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_crypto_funding_observations_and_native_acceptance():
 for path in ('tests/test_crypto_funding_observations.py','tests/test_crypto_funding_predecessor.py','aws/ops/checks/test_crypto_funding_acceptance.py'):
  subprocess.run([sys.executable,str(R/path)],cwd=R,check=True)
