"""Original provider flow and actual handler regressions; no cloud calls."""
from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'aws/shared/tests'), str(ROOT / 'tests')]
if __name__ == '__main__':
    from test_provider_flow_research import OriginalFlow, GroupFlow
    from test_provider_flow_store import OriginalCollection, RetainedFlow
    from provider_flow_test_support import NativeHandlers, FlowBoundaries
    suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(t) for t in
        (OriginalFlow, GroupFlow, OriginalCollection, RetainedFlow, NativeHandlers, FlowBoundaries))
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful(): raise SystemExit(1)
