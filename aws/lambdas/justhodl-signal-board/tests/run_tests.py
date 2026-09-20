from pathlib import Path
import sys, unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'aws/shared'))
sys.path.insert(0,str(ROOT/'aws/shared/tests'))
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]/"tests"))
    from aaii_consumer_test_support import run as run_aaii
    run_aaii()

if __name__=='__main__':
    sys.path.insert(0,str(ROOT/'tests'))
    from funding_consumer_test_support import run
    run('justhodl-signal-board')
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_inflection_authority.py')
    from breadth_consumer_test_support import ConsumerTests
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerTests))
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
