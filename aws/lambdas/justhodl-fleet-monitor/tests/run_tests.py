"""Deployment workflow bridge to the shared real-handler regression suite."""
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).resolve().parents[4] / "tests/test_fleet_monitor_privacy.py"), run_name="__main__")
