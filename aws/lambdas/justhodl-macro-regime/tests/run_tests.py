"""Offline actual-handler provider diagnostics regressions."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "aws/shared")]
from provider_metrics_privacy_helpers import run

if __name__ == "__main__":
    run("justhodl-macro-regime")
