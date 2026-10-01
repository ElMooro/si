"""Offline Mode A withdrawal and paired consumer regressions."""
import subprocess
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
subprocess.run([sys.executable, str(ROOT / "tests/test_harness_mode_a_withdrawal.py")], check=True)
