from pathlib import Path
import sys, unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'aws/shared/tests'))
if __name__=='__main__':
    suite=unittest.defaultTestLoader.discover(str(ROOT/'aws/shared/tests'),pattern='test_hot_money_authority.py')
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from extremes_consumer_test_support import run as run_extremes_consumers
    run_extremes_consumers()
