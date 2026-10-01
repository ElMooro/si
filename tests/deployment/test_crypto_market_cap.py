"""Exercise descriptive semantics, consumer closure and native acceptance offline."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_crypto_market_cap_semantics_and_native_acceptance():
 for p in ('tests/test_crypto_market_cap.py','aws/ops/checks/test_crypto_market_cap_acceptance.py'):
  subprocess.run([sys.executable,str(R/p)],cwd=R,check=True)
