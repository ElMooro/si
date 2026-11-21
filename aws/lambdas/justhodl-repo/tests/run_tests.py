"""Network-free production macro donor builders and capacity handler regressions; write-only-on-change of the history files."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from macro_donor_test_support import run
import test_history_writes
if __name__=="__main__":
    test_history_writes.main()
    run()
