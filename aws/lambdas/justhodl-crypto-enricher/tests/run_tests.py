"""Offline output ownership regression checks."""
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from test_engine_output_ownership import OutputOwnershipTests
if __name__ == "__main__":
    suite = unittest.TestSuite(OutputOwnershipTests(name) for name in ['test_crypto_full_handler_rebases_on_new_v10_without_losing_prices', 'test_crypto_rejects_wrong_base_and_exhausted_races'])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
