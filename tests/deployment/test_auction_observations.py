"""Run the actual auction observation and acquisition regressions in deployment."""
from pathlib import Path
import runpy
globals().update({k:v for k,v in runpy.run_path(str(Path(__file__).resolve().parents[1]/'test_auction_observations.py')).items() if k.startswith('test_')})
