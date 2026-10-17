"""Deployment workflow entrypoint: real-handler privacy tests + ops 6528 billable-read regressions."""
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[4]
suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_fleet_error_monitor_privacy.py")
suite.addTests(unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_fleet_error_monitor_reads.py"))
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
