"""Capital-structure decision boundaries; no external services."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests'),str(ROOT/'aws/shared/tests')]
if __name__=='__main__':
    suite=unittest.TestSuite()
    suite.addTests(unittest.defaultTestLoader.discover(str(ROOT/'aws/shared/tests'),pattern='test_risk_gate_authority.py'))
    for pattern in ('test_capital_structure_context.py','test_capital_structure_consumers.py'):
        suite.addTests(unittest.TestLoader().discover(str(ROOT/'tests'),pattern=pattern))
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
