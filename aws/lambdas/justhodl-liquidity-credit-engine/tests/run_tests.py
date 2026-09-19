"""Original-source LCE projection, publication and active-handler regressions."""
import sys
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(Path(__file__).resolve().parent)]
if __name__=='__main__':
    suite=unittest.TestSuite()
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_lce_research_model.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent),pattern='test_lce_research_store.py'))
    import macro_donor_test_support
    suite.addTests(unittest.TestLoader().loadTestsFromModule(macro_donor_test_support))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
