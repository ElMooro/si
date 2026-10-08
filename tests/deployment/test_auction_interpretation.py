from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]

def test_auction_interpretation_regressions():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_interpretation.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==8
    for test in tests:test()
