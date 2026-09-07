"""Bridge test wiring -- the fixtures live in jh_fixtures.py so the fusion suite can share them."""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
from jh_fixtures import *  # noqa: F401,F403,E402
