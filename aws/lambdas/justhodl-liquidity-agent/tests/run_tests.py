"""Native and preserved predecessor checks; no provider or AWS access."""
from pathlib import Path
import subprocess,sys
HERE=Path(__file__).parent
for name in ("test_legacy_contract.py","test_agent_native.py"):
    subprocess.run([sys.executable,str(HERE/name)],check=True)
