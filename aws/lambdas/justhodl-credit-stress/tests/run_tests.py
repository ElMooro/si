"""Native credit arithmetic, source replay and publication fault checks."""
from pathlib import Path
import sys,unittest
if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from credit_consumer_test_support import run as run_credit_consumers
    run_credit_consumers()

if __name__=='__main__':
    root=Path(__file__).resolve().parent;sys.path.insert(0,str(root))
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(root),pattern='*_credit_tests.py'))
    if not result.wasSuccessful():sys.exit(1)
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from dealer_consumer_test_support import run as run_dealer
    sys.exit(0 if run_dealer('credit-stress') else 1)
