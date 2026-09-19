"""Canonical CISS producer, original-source storage and consumer contracts."""
import sys
import unittest
from pathlib import Path
shared=Path(__file__).resolve().parents[3]/"shared"
sys.path[:0]=[str(shared),str(shared/"tests")]
from ciss_vintage_test_support import run
if __name__=="__main__":
    run()
    suite=unittest.TestLoader().discover(str(shared/"tests"),pattern="test_ciss_source_*.py")
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
