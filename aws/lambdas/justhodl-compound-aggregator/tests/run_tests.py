from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests')]
suite = unittest.TestSuite(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'), pattern=name) for name in ('test_holdings_derived_boundary.py', 'test_compound_numeric.py'))
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
