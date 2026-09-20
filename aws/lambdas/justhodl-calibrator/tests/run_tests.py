"""Actual calibrator exclusion path; no AWS reads or writes."""
from pathlib import Path
import sys,unittest
root=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(root/'aws/lambdas/justhodl-crisis-plumbing/tests'))
suite=unittest.defaultTestLoader.loadTestsFromName('test_consumer_boundaries.ConsumerBoundaries.test_actual_calibrator_excludes_legacy_outcomes_without_modifying_ledger_rows')
result=unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
