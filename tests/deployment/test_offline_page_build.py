"""Keep the source-only site boundary in both deploy gates."""
from pathlib import Path
import subprocess
import sys


def test_offline_page_build_boundary_and_complete_invented_site():
    root = Path(__file__).resolve().parents[2]
    subprocess.run([sys.executable, str(root / "tests/test_offline_pages.py")], cwd=root, check=True)
