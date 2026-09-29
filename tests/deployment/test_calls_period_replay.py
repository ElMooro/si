from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from calls_period_replay_tests import CallsPeriods

def test_calls_frozen_periods():
    result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromTestCase(CallsPeriods))
    assert result.wasSuccessful()
