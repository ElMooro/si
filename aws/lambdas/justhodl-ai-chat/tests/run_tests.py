"""Offline downstream private-source and historical response checks."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests"))
from downstream_privacy_test_support import run
if __name__ == "__main__":
    run("ai-chat")

    from tenor_consumer_test_support import run as run_tenor
    run_tenor("ai-chat")

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(ROOT/"tests"))
    from research_brief_consumer_test_support import run as run_research
    run_research("ai-chat")

if __name__ == "__main__":
    from daily_macro_consumer_test_support import run as run_daily
    run_daily("ai-chat")
