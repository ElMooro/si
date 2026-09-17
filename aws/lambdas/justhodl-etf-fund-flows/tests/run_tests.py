"""Offline actual-handler provider diagnostics regressions."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "aws/shared")]
from provider_metrics_privacy_helpers import run

if __name__ == "__main__":
    run("justhodl-etf-fund-flows")
    import unittest
    suite=unittest.defaultTestLoader.discover(str(Path(__file__).parent),pattern='test_dated_flows.py')
    if not unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful():raise SystemExit(1)
