from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_calls_original_binding_and_native_audit_with_complete_retained_originals():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_calls_original_binding.py')],cwd=ROOT,
        capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stdout+result.stderr
