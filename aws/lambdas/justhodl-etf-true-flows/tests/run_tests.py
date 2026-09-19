from pathlib import Path
import sys,unittest
here=Path(__file__).resolve().parent
sys.path[:0]=[str(here.parent/'source'),str(here.parents[2]/'shared'),str(here)]
suite=unittest.defaultTestLoader.discover(str(here),'test_*.py')
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
