"""Isolated SEC 10-K/Q returned-form and native-entry acceptance."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / 'tests'))
from sec_atom_test_support import run
if __name__ == '__main__':
    sys.exit(run('10kq'))
