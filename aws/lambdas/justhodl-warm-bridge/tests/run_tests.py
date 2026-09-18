import sys
import unittest
from pathlib import Path
root = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(root / "source"), str(root.parents[1] / "shared")]
suite = unittest.defaultTestLoader.discover(str(root / "tests"), pattern="test_*.py")
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
