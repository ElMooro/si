from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests')]
suite = unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'), pattern='test_holdings_derived_boundary.py')
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
