from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(name) for name in
    ('test_futures_source_capture','test_futures_source_preflight','test_futures_session_calendar','test_futures_research_model','test_futures_calculation_candidate','test_futures_research_store','test_futures_native_candidate','test_futures_research_context','test_futures_native_handler','test_futures_native_acceptance','test_futures_research_consumers'))
if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():sys.exit(1)
