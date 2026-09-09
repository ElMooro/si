"""Offline owner authorization and concurrent-write checks."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from watchlist_privacy_test_support import run
if __name__ == "__main__":
    run("watchlist")
