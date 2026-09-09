"""Offline actual-handler integration regression (shared fixture, no AWS)."""
import importlib.util
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("integration_suite",HERE.parents[1]/"justhodl-portfolio-admin/tests/run_tests.py")
suite=importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)
if __name__ == "__main__":
    suite.test_actual_handler_handles_mixed_priced_unpriced_book()
    print("justhodl-portfolio-snapshot integration test passed")
