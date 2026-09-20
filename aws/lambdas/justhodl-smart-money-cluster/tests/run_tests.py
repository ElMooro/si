from pathlib import Path
import sys
import unittest
HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent/'source'), str(HERE.parents[2]/'shared')]
suite = unittest.TestLoader().discover(str(HERE), pattern='test_*.py')
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
