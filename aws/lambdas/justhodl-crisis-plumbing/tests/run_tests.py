"""Offline original-source, storage and consumer acceptance; no cloud calls."""
from pathlib import Path
import sys,unittest
root=Path(__file__).resolve().parent
sys.path.insert(0,str(root))
result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(root),pattern='test_*.py'))
sys.exit(0 if result.wasSuccessful() else 1)
