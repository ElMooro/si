from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
names=('test_fx_quote_capture','test_fx_source_preflight','test_fx_research_model','test_fx_calculation_candidate',
    'test_fx_research_store','test_fx_native_candidate','test_fx_research_consumers','test_fx_native_handler','test_fx_native_acceptance')
suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(name) for name in names)
if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():sys.exit(1)
