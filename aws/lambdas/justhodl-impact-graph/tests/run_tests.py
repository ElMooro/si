"""Capital-structure consumer boundaries and existing engine regressions; no AWS."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
if __name__=='__main__':
    from test_capital_structure_remaining_consumers import Tests
    suites=[unittest.defaultTestLoader.loadTestsFromTestCase(Tests)]
    from provider_flow_test_support import FlowBoundaries
    suites.append(unittest.defaultTestLoader.loadTestsFromTestCase(FlowBoundaries))
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(suites))
    if not result.wasSuccessful():raise SystemExit(1)
