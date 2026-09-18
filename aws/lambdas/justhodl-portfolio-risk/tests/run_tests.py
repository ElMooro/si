"""Full private-publication and HTTP authorization checks; no real services."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))
from private_portfolio_test_support import run
if __name__ == "__main__":
    run("portfolio-risk")
    import subprocess
    subprocess.run([sys.executable, str(Path(__file__).with_name('test_risk_model.py'))], check=True)
