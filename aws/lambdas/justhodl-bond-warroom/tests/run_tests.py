"""Network-free production macro donor builders and capacity handler regressions."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
from macro_donor_test_support import run
if __name__=="__main__":
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from funding_consumer_test_support import run as funding_tests
    funding_tests('justhodl-bond-warroom')
    run()
