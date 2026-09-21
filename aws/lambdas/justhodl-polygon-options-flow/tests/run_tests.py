from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
if __name__=='__main__':
    suite=unittest.TestSuite()
    for pattern in ('test_option_flow*.py','test_option_contract_research.py','test_option_research_rows.py','test_option_snapshot_capture.py'):
        suite.addTests(unittest.defaultTestLoader.discover(str(ROOT/'tests'),pattern=pattern))
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
