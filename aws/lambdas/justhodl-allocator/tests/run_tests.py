from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/"tests"))
from tenor_consumer_test_support import run
if __name__=="__main__":run("allocator")

if __name__=="__main__":
    from lce_consumer_test_support import run as run_lce
    run_lce("allocator")

if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/"tests"))
    from eurodollar_consumer_test_support import run as run_eurodollar_consumers
    run_eurodollar_consumers()

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from extremes_consumer_test_support import run as run_extremes_consumers
    run_extremes_consumers()
