"""Offline AAII research consumer boundaries."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[4]/'tests'))
from aaii_consumer_test_support import run
if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from credit_consumer_test_support import run as run_credit_consumers
    run_credit_consumers()

if __name__ == '__main__': run()

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from insider_consumer_test_support import run as run_insider_consumers
    run_insider_consumers()

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from extremes_consumer_test_support import run as run_extremes_consumers
    run_extremes_consumers()
    from extremes_native_test_support import run as run_extremes_native
    run_extremes_native()
