from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_complete_ciss_original_candidate_and_independent_arithmetic():
    result = subprocess.run([sys.executable, str(ROOT/'tests/test_calls_ciss_lineage.py')],
                            cwd=ROOT, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout+result.stderr
