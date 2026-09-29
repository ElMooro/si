from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]

def test_actual_auction_transport_and_native_adapter_regressions():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_fred_transport.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==18
    for test in tests:test()
