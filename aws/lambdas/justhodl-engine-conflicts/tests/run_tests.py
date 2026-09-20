"""Production consumer qualification checks with offline source fixtures."""
from pathlib import Path
import sys, unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
if __name__=='__main__':
    suite=unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_conflicts_and_narrative_do_not_invent_trades_from_legacy_capital'))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
