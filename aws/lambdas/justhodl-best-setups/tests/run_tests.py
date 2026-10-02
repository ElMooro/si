"""Risk Gate authority boundary regressions, no external services."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(ROOT/'tests')]
if __name__=='__main__':
    from retail_consumer_test_support import Boundaries
    from sector_consumer_test_support import SectorBoundaries
    suite=unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_risk_gate_authority.py')
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_best_setups_full_handler_legacy_scores_cannot_add_names_or_signals'))
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_best_setups_rejects_legacy_flow_hidden_behind_new_envelope'))
    suite.addTests(unittest.TestLoader().loadTestsFromName('test_fabric_numeric.Whole.test_real_producer_to_real_best_setups_cannot_change_rank_or_seed_names'))
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(Boundaries))
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(SectorBoundaries))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
