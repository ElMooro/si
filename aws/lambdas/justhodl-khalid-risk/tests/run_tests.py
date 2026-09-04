import runpy
from pathlib import Path
scope=runpy.run_path(str(Path(__file__).with_name("test_risk_engine.py")))
tests=sorted((n,f) for n,f in scope.items() if n.startswith("test_") and callable(f))
for name,test in tests: test()
print(f"Khalid Risk tests passed: {len(tests)}")
