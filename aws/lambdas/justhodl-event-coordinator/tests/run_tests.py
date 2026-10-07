"""Run coordinator delivery regressions in the normal Lambda deploy lane."""
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[4]
suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'), pattern='test_event_coordinator_delivery.py')
suite.addTests(unittest.defaultTestLoader.discover(str(ROOT / 'tests'), pattern='test_master_ranker_schedule.py'))
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
