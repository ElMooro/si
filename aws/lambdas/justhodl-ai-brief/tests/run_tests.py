"""Run the producer and cross-consumer Calls regression contract."""
import runpy
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
runpy.run_path(str(ROOT / "tests/private_artifact_test_support.py"))["run"]("justhodl-ai-brief")
runpy.run_path(str(ROOT / 'tests/deployment/test_calls_integrity.py'), run_name='__main__')
runpy.run_path(str(Path(__file__).with_name('free_brief_tests.py')), run_name='__main__')
runpy.run_path(str(Path(__file__).with_name('replay_tests.py')), run_name='__main__')

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(ROOT/"tests"))
    from research_brief_consumer_test_support import run as run_research
    run_research("ai-brief")

if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/"tests"))
    from eurodollar_consumer_test_support import run as run_eurodollar_consumers
    run_eurodollar_consumers()

if __name__ == '__main__':
    import sys,unittest
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from sector_consumer_test_support import SectorBoundaries
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SectorBoundaries))
    if not result.wasSuccessful():raise SystemExit(1)
