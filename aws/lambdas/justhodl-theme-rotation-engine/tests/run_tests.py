"""Offline actual theme writer and schema-consumer regressions."""
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from test_theme_output_ownership import ThemeOutputOwnershipTests
if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ThemeOutputOwnershipTests))
    raise SystemExit(0 if result.wasSuccessful() else 1)
