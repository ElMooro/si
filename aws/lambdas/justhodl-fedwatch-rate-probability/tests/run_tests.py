from pathlib import Path
import sys,unittest
here=Path(__file__).resolve().parent
if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().discover(str(here),pattern='test_*.py'))
    sys.exit(0 if result.wasSuccessful() else 1)
