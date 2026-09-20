from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'aws/shared/tests'))
if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from credit_consumer_test_support import run as run_credit_consumers
    run_credit_consumers()

if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]/"tests"))
    from aaii_consumer_test_support import run as run_aaii
    run_aaii()

if __name__=='__main__':
    suite=unittest.defaultTestLoader.discover(str(ROOT/'aws/shared/tests'),pattern='test_global_liquidity_authority.py')
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
