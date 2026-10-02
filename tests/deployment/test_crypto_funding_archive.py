"""Public funding original replay and code acceptance, invented fixtures only."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_crypto_funding_original_store_and_acceptance():
 for path in ('tests/test_crypto_funding_archive.py','tests/test_options_coverage_publication.py','aws/ops/checks/test_crypto_funding_archive_acceptance.py'):
  subprocess.run([sys.executable,str(R/path)],cwd=R,check=True)
