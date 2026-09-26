"""Offline native Bond Vol regression checks."""
from pathlib import Path
import subprocess,sys
subprocess.run([sys.executable,str(Path(__file__).parent/'test_bond_native.py')],check=True)
