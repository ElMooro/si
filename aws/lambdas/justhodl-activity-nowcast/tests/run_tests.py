from pathlib import Path
import sys,unittest
HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(HERE),str(HERE.parent/'source'),str(HERE.parents[3]/'aws/shared')]
if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(HERE),pattern='test_native.py'))
    sys.exit(0 if result.wasSuccessful() else 1)
