"""Original holdings reconstruction, replay and actual handler regression."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(ROOT/'tests')]
if __name__=='__main__':
    suite=unittest.TestSuite()
    for name in ('test_etf_holdings_native.Holdings','test_etf_holdings_model.Projection','test_etf_holdings_model.Collection',
                 'test_etf_holdings_store.RetainedHoldings','etf_holdings_test_support.NativeHandlers'):
        suite.addTests(unittest.defaultTestLoader.loadTestsFromName(name))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
