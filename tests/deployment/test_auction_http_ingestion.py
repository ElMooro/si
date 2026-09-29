from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]


def test_treasury_http_faults_cannot_masquerade_as_complete_measurements():
    scope=runpy.run_path(str(ROOT/'tests/test_auction_http_ingestion.py'))
    tests=[fn for name,fn in scope.items() if name.startswith('test_')]
    assert len(tests)==11
    for test in tests:test()
