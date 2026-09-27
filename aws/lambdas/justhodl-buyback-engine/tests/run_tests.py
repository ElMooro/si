from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
from test_capital_structure_final_consumers import Tests
from test_measurements import MeasurementTests
if __name__=='__main__':
    suite=unittest.TestSuite()
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(MeasurementTests))
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
