from pathlib import Path
import subprocess
import sys


def test_dependency_lineage_preserves_shared_roots_cycles_and_whole_populations():
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run([sys.executable, str(root/'tests/test_dependency_lineage.py')], cwd=root,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
