"""Dependency-free runner: the factory contract suite plus the discipline/evidence/prints suite (2026-09-13)."""
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
rc = 0
for name in ('test_factory.py', 'test_discipline.py'):
    try:
        runpy.run_path(str(ROOT / 'tests' / 'factory' / name), run_name='__main__')
    except SystemExit as exc:            # unittest.main() exits per file; aggregate instead of stopping at the first
        rc |= int(exc.code or 0)
sys.exit(rc)
