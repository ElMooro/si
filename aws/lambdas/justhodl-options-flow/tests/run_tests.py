"""Offline output ownership regression checks."""
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from test_engine_output_ownership import OutputOwnershipTests
if __name__ == "__main__":
    suite = unittest.TestSuite(OutputOwnershipTests(name) for name in ['test_macro_flow_actual_publication_block_never_overwrites_scanner', 'test_premium_consumers_use_nested_macro_rows_and_correct_ratio_direction'])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
