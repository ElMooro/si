from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]

def test_whole_calendar_response_and_native_failure_regressions():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_calendar.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==15
    for test in tests:test()
