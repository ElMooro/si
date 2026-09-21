"""Production consumer qualification checks with offline source fixtures."""
from pathlib import Path
import sys, unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(ROOT/'tests')]
if __name__=='__main__':
    suite=unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_equity_no_direct_capital_family_or_scorecard_auto_promotion'))
    suite.addTests(unittest.defaultTestLoader.loadTestsFromName('etf_holdings_test_support.NativeHandlers'))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
