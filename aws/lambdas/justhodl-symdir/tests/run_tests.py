import sys,unittest
from pathlib import Path
root=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(root/'aws/shared'),str(root/'aws/shared/tests'),str(root/'tests')]
from treasury_consumer_test_support import SymdirFiscalTests
suite=unittest.defaultTestLoader.loadTestsFromTestCase(SymdirFiscalTests)
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
