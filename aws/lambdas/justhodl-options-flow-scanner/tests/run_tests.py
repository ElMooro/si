"""Offline output ownership regression checks."""
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from test_engine_output_ownership import OutputOwnershipTests
if __name__ == "__main__":
    suite = unittest.TestSuite(OutputOwnershipTests(name) for name in ['test_scanner_full_handler_owns_new_key_even_with_legacy_environment', 'test_source_keys_and_dedicated_pages_match_each_schema'])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
