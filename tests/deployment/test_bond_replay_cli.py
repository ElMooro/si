"""Keep current Bond Desk publication acceptance in the deployment gate."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_bond_publication_replay_rejects_corruption_without_network_or_writes():
    result = subprocess.run([sys.executable, str(ROOT / 'tests/test_bond_public_replay.py')],
                            cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
