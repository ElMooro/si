from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/"tests"))
from tenor_consumer_test_support import run
if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from credit_consumer_test_support import run as run_credit_consumers
    run_credit_consumers()

if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]/"tests"))
    from aaii_consumer_test_support import run as run_aaii
    run_aaii()

if __name__=="__main__":run("morning-intelligence")

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(ROOT/"tests"))
    from research_brief_consumer_test_support import run as run_research
    run_research("morning-intelligence")

if __name__ == "__main__":
    import unittest
    sys.path.insert(0,str(ROOT/'aws/shared/tests'))
    suite=unittest.defaultTestLoader.discover(str(ROOT/'aws/shared/tests'),pattern='test_hot_money_authority.py')
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():sys.exit(1)
    from dealer_consumer_test_support import run as run_dealer
    sys.exit(0 if run_dealer('morning-intelligence') else 1)
