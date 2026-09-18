from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests'), str(Path(__file__).resolve().parent)]
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().discover(str(ROOT/'aws/shared/tests'), pattern='test_research_brief_model.py'))
    suite.addTests(unittest.TestLoader().discover(str(Path(__file__).resolve().parent), pattern='test_research_brief_store.py'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
