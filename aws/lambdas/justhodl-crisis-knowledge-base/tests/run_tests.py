from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))

if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]/"tests"))
    from aaii_consumer_test_support import run as run_aaii
    run_aaii()

if __name__ == "__main__":
    from daily_macro_consumer_test_support import run as run_daily
    run_daily("crisis-knowledge-base")
