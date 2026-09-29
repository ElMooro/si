from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_calls_original_fr2004_binding_and_native_audit_boundary():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_calls_fails_binding.py')],cwd=ROOT,
        capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stdout+result.stderr
