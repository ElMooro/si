"""Run source and consumer regressions with invented data only."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[4]
subprocess.run([sys.executable,str(R/'tests/test_crypto_market_cap.py')],cwd=R,check=True)
subprocess.run([sys.executable,str(R/'tests/test_crypto_funding_observations.py')],cwd=R,check=True)
