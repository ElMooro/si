"""Offline original archive and store regressions."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).resolve().parents[1]/'source'),str(Path(__file__).parent)]
if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().discover(str(Path(__file__).parent),pattern='test_*.py'))
    sys.exit(0 if result.wasSuccessful() else 1)
