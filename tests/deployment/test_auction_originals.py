"""Exercise original byte retention/replay and real handler publication ordering."""
from pathlib import Path
import runpy

globals().update({key: value for key, value in runpy.run_path(
    str(Path(__file__).resolve().parents[1]/'test_auction_originals.py')).items() if key.startswith('test_')})
