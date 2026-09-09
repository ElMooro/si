"""Offline actual-handler integration regression (shared fixture, no AWS)."""
import importlib.util
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("integration_suite",HERE.parents[1]/"justhodl-backtest-engine/tests/run_tests.py")
suite=importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)
if __name__ == "__main__":
    suite.test_real_snapshotter_preserves_same_week_and_same_second_versions(suite._load())
    print("justhodl-calibration-snapshotter integration test passed")
