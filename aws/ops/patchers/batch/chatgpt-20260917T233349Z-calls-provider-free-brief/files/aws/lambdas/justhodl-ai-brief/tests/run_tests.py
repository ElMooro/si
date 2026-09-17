"""Run the producer and cross-consumer Calls regression contract."""
import runpy
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
runpy.run_path(str(ROOT / "tests/private_artifact_test_support.py"))["run"]("justhodl-ai-brief")
runpy.run_path(str(ROOT / 'tests/deployment/test_calls_integrity.py'), run_name='__main__')
runpy.run_path(str(Path(__file__).with_name('free_brief_tests.py')), run_name='__main__')
