import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
code = 0
for test in sorted(HERE.glob("test_*.py")):
    code |= subprocess.call([sys.executable, str(test)])
sys.exit(code)
