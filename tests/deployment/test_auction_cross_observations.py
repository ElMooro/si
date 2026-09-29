from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]

def test_dated_cross_observations_and_internal_consumer_regressions():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_cross_observations.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==18
    for test in tests:test()
