from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))

if __name__ == "__main__":
    from daily_macro_consumer_test_support import run as run_daily
    run_daily("khalid-adaptive")
