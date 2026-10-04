from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'source'))
sys.path.insert(0,str(Path(__file__).resolve().parent))
suite=unittest.defaultTestLoader.discover(str(Path(__file__).resolve().parent),pattern='test_*.py')
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
