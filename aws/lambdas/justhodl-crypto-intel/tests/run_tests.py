"""Run source and consumer regressions with invented data only."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[4]
subprocess.run([sys.executable,str(R/'tests/test_crypto_market_cap.py')],cwd=R,check=True)
subprocess.run([sys.executable,str(R/'tests/test_crypto_funding_observations.py')],cwd=R,check=True)
subprocess.run([sys.executable,str(R/'tests/test_crypto_funding_archive.py')],cwd=R,check=True)
for name in ('model','transport','archive','consumers','predecessor'):
 subprocess.run([sys.executable,str(R/('tests/test_crypto_stablecoin_'+name+'.py'))],cwd=R,check=True)

for name in ('model','transport','archive','consumers'):
 subprocess.run([sys.executable,str(R/('tests/test_crypto_sentiment_'+name+'.py'))],cwd=R,check=True)
