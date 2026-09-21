from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/"tests"))
from tenor_consumer_test_support import run
if __name__=="__main__":
    run("daily-report-v3")
    sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).resolve().parent)]
    suite=unittest.TestSuite()
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_report_observations.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent),pattern='test_report_source_store.py'))
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_daily_macro_model.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent),pattern='test_daily_macro_store.py'))
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'),pattern='test_daily_market_model.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent),pattern='test_daily_market_store.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent),pattern='test_activity_source_catalog.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent),pattern='test_dollar_source_catalog.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent),pattern='test_sector_source_catalog.py'))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
