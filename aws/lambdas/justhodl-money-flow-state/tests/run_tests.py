"""Exercise actual pure compiler, source replay and publication boundaries without AWS."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared/tests'),str(ROOT/'aws/shared')]
from test_money_volume_research import Arithmetic,Publication
suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in (Arithmetic,Publication))
if __name__=='__main__':
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():raise SystemExit(1)
