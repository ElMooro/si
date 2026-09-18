import sys, unittest
from pathlib import Path
root=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(root/'shared'),str(root/'shared/tests')]
suite=unittest.TestLoader().discover(str(root/'shared/tests'),pattern='test_treasury_fiscal_*.py')
suite.addTests(unittest.TestLoader().discover(str(Path(__file__).parent),pattern='test_*.py'))
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
