"""Offline AAII research consumer boundaries."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[4]/'tests'))
from aaii_consumer_test_support import run
if __name__ == '__main__': run()
