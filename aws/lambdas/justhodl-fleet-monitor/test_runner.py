"""Offline real-handler privacy regression entrypoint."""
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).resolve().parents[3] / "tests/test_fleet_monitor_privacy.py"), run_name="__main__")
