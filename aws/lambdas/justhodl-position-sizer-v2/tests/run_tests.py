"""Run Calls abstention, expiry and risk-cap consumer regression tests."""
import runpy
from pathlib import Path
runpy.run_path(str(Path(__file__).resolve().parents[4] / 'tests/deployment/test_calls_integrity.py'), run_name='__main__')
