"""Actual CISS consumer regression and source-bound interpretation tests."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
from ciss_readthrough_test_support import run
if __name__=="__main__":run()
