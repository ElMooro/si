from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]


def test_actual_calls_ciss_original_binding_cache_expiry_and_native_auditor():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_calls_ciss_binding.py')],cwd=ROOT,
                          capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stdout+result.stderr
