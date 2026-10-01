from pathlib import Path
from unittest.mock import patch
import sys,unittest
root=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(root/'aws/shared'),str(root/'aws/shared/tests'),str(root/'tests'),
              str(Path(__file__).resolve().parent),str(Path(__file__).resolve().parents[1]/'source')]
from treasury_consumer_test_support import SymdirFiscalTests
import test_universal_provider_search,test_warehouse_routing,test_directory_identity,test_directory_native
loader=unittest.defaultTestLoader
suite=unittest.TestSuite([loader.loadTestsFromTestCase(SymdirFiscalTests)])
for module in (test_universal_provider_search,test_warehouse_routing,test_directory_identity,test_directory_native):
    suite.addTests(loader.loadTestsFromModule(module))
with patch('urllib.request.urlopen',side_effect=AssertionError('Real HTTP forbidden in native regressions')):
    result=unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
