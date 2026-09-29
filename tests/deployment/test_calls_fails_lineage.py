from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_calls_complete_fr2004_original_candidate_and_independent_integer_checks():
    result = subprocess.run([sys.executable, str(ROOT/'tests/test_calls_fails_lineage.py')],
        cwd=ROOT, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout+result.stderr
