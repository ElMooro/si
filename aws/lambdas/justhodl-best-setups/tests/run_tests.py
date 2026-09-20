"""Risk Gate authority boundary regressions, no external services."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
if __name__=='__main__':
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_risk_gate_authority.py')
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_best_setups_full_handler_legacy_scores_cannot_add_names_or_signals'))
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_best_setups_rejects_legacy_flow_hidden_behind_new_envelope'))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

if __name__=='__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from retail_consumer_test_support import run as run_retail
    run_retail()
