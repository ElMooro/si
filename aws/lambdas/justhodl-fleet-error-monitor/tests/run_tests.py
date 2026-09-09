"""Deployment workflow entrypoint for the real-handler privacy tests."""
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).resolve().parents[4] / "tests/test_fleet_error_monitor_privacy.py"), run_name="__main__")
