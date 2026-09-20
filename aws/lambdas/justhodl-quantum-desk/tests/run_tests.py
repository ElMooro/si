"""Risk Gate authority boundary regressions, no external services."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
if __name__=='__main__':
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_risk_gate_authority.py')
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():raise SystemExit(1)

if __name__=='__main__':
    import subprocess,sys
    from pathlib import Path
    subprocess.run([sys.executable,str(Path(__file__).resolve().parents[4]/'tests/tail_consumer_test_support.py')],check=True)
