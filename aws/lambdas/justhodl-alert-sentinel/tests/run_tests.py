from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'tests'))
from dealer_consumer_test_support import run
if __name__=='__main__':sys.exit(0 if run('alert-sentinel') else 1)
