from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]
def test_complete_forward_heuristic_population_and_eligibility_regressions():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_forward.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==16
    for test in tests:test()
