import runpy
from pathlib import Path
scope=runpy.run_path(str(Path(__file__).with_name("test_treasury.py")))
tests=sorted((n,f) for n,f in scope.items() if n.startswith("test_") and callable(f))
for name,test in tests: test()
print(f"Settlement Treasury tests passed: {len(tests)}")
import unittest
new=runpy.run_path(str(Path(__file__).with_name('test_research.py')))
result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromTestCase(new['Research']))
if not result.wasSuccessful():raise SystemExit(1)
