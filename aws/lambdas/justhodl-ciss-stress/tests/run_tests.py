"""Run the canonical CISS producer/consumer contracts."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
from ciss_vintage_test_support import run
if __name__=="__main__":run()
