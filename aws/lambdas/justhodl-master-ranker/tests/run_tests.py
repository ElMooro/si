"""Risk Gate authority boundary regressions, no external services."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
if __name__=='__main__':
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_risk_gate_authority.py')
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_inflection_authority.py'))
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_holdings_authority.py'))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
