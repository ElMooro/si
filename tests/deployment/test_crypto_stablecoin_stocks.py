"""Original stock semantics and native release checks; invented data only."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_stablecoin_stock_semantics_original_replay_and_acceptance():
 for name in ('model','transport','archive','consumers','predecessor'):
  subprocess.run([sys.executable,str(R/('tests/test_crypto_stablecoin_'+name+'.py'))],cwd=R,check=True)
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_crypto_stablecoin_stock_acceptance.py')],cwd=R,check=True)
