"""Fusion test wiring -- shares the bridge suite's fixtures (realistic engine artifacts, fake S3)."""
import sys
from pathlib import Path

_BRIDGE_TESTS = Path(__file__).resolve().parents[2] / "justhodl-jhsignal-bridge" / "tests"
if str(_BRIDGE_TESTS) not in sys.path:
    sys.path.insert(0, str(_BRIDGE_TESTS))
from jh_fixtures import *  # noqa: F401,F403,E402
