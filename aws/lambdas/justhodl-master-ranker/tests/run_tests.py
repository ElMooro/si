"""Risk Gate authority boundary regressions, no external services."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
if __name__=='__main__':
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_risk_gate_authority.py')
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_inflection_authority.py'))
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_holdings_authority.py'))
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_holdings_derived_boundary.py'))
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_flow_handler_excludes_universe_score_agreement_and_old_downstream_revision'))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from extremes_consumer_test_support import run as run_extremes_consumers
    run_extremes_consumers()
