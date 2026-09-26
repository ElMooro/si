#!/usr/bin/env python3
"""Offline full-handler private-artifact regression gate."""
import runpy
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
runpy.run_path(str(ROOT / "tests/private_artifact_test_support.py"))["run"]("justhodl-ask-desk")

if __name__ == '__main__':
    from pathlib import Path
    import subprocess, sys
    subprocess.run([sys.executable,str(Path(__file__).resolve().parents[4]/'tests/test_signal_board_interpretation_consumers.py')],check=True)
