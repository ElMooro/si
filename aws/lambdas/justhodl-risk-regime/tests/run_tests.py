from pathlib import Path
import sys, unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'aws/shared/tests'))
if __name__=='__main__':
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_inflection_authority.py')
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
