from pathlib import Path
import sys, unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(Path(__file__).parent)]
if __name__=='__main__':
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_inflection_authority.py')
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_global_liquidity_authority.py'))
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_hot_money_authority.py'))
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_risk_regime_authority'))
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_native_research'))
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
