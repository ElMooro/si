"""Full private-publication and HTTP authorization checks; no real services."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from private_portfolio_test_support import run
if __name__ == "__main__":
    run("behavior-mirror")
