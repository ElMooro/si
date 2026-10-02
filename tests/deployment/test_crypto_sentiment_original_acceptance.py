"""Original sentiment replay uses only invented storage and source fixtures."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_crypto_sentiment_original_archive_acceptance():
 for path in ('aws/ops/checks/test_crypto_sentiment_original_acceptance.py','aws/ops/checks/test_crypto_sentiment_original_operation.py'):
  subprocess.run([sys.executable,str(R/path)],cwd=R,check=True)
