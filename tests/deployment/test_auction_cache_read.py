from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]


def test_treasury_cache_faults_preserve_history_and_public_snapshot():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_cache_read.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==9
    for test in tests:test()
