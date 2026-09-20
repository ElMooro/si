#!/usr/bin/env python3
"""Offline full-handler private-artifact regression gate."""
import runpy
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
runpy.run_path(str(ROOT / "tests/private_artifact_test_support.py"))["run"]("justhodl-ask")

import sys, unittest
sys.path[:0]=[str(ROOT/"aws/shared"),str(ROOT/"aws/shared/tests")]
suite=unittest.TestSuite()
suite.addTests(unittest.TestLoader().loadTestsFromName('test_capital_research_boundary.Tests.test_ask_context_never_supplies_legacy_capital_picks_to_language_model'))
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
