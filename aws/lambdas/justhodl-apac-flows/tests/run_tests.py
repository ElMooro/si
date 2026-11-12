"""Provider-flow decision boundary regression; no AWS calls."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/"tests"))
from provider_flow_test_support import FlowBoundaries
if __name__ == "__main__":
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(FlowBoundaries))
    if not result.wasSuccessful():raise SystemExit(1)
    import subprocess
    if subprocess.call([sys.executable, str(Path(__file__).resolve().parent / "test_japan_jpx.py")]):raise SystemExit(1)
