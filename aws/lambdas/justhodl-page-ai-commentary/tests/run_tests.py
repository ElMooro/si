"""Offline downstream private-source and historical response checks."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from downstream_privacy_test_support import run
if __name__ == "__main__":
    run("page-ai-commentary")

if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/"tests"))
    from eurodollar_consumer_test_support import run as run_eurodollar_consumers
    run_eurodollar_consumers()

if __name__ == '__main__':
    from pathlib import Path
    import subprocess, sys
    subprocess.run([sys.executable,str(Path(__file__).resolve().parents[4]/'tests/test_signal_board_interpretation_consumers.py')],check=True)
