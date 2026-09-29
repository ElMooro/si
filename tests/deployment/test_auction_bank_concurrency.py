from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]


def test_owned_auction_banks_preserve_concurrent_updates():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_bank_concurrency.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==10
    for test in tests:test()
