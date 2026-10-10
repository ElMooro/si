
if __name__=='__main__':
    from pathlib import Path
    import sys
    import unittest
    from test_email_display import EmailDisplay
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(EmailDisplay))
    if not result.wasSuccessful():raise SystemExit(1)
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from retail_consumer_test_support import run as run_retail
    run_retail()
