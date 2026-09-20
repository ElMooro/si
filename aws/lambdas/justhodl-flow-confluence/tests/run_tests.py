from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests')]
suite = unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'), pattern='test_holdings_derived_boundary.py')
suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_flow_handler_excludes_universe_score_agreement_and_old_downstream_revision'))
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
