"""Offline native and conserved predecessor checks."""
from pathlib import Path
import subprocess,sys
for name in ("test_legacy_contract.py","test_curve_native.py"):
    subprocess.run([sys.executable,str(Path(__file__).parent/name)],check=True)
