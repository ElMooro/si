"""Complete original sentiment, consumer and history semantics; invented I/O only."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_crypto_sentiment_originals_and_consumers():
 for name in ('model','transport','archive','consumers'):
  subprocess.run([sys.executable,str(R/('tests/test_crypto_sentiment_'+name+'.py'))],cwd=R,check=True)
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_crypto_sentiment_acceptance.py')],cwd=R,check=True)
