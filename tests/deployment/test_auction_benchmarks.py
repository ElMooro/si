"""Execute the full pure and native-adapter benchmark regressions in the gate."""
from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]
def test_complete_auction_benchmark_regressions():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_benchmarks.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==15
    for test in tests:test()
