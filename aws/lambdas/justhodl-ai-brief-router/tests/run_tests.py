"""Deployment bridge to the offline actual-handler context contract suite."""
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).resolve().parents[4] / "tests/test_sniffer_contexts.py"), run_name="__main__")
