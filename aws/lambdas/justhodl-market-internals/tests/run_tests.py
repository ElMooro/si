"""Offline native calculations, retention, replay and consumer compatibility."""
from pathlib import Path
import sys, unittest
if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    sys.path.insert(0, str(root))
    suite = unittest.defaultTestLoader.discover(str(root), pattern='*breadth_tests.py')
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
