from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]

def test_dated_complete_auction_concerns_and_actual_native_regressions():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_concerns.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==19
    for test in tests:test()
