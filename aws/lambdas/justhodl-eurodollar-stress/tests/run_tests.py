from pathlib import Path
import sys,unittest
if __name__=='__main__':
    root=Path(__file__).resolve().parent;sys.path.insert(0,str(root))
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(root),pattern='*_eurodollar_tests.py'))
    sys.exit(0 if result.wasSuccessful() else 1)
