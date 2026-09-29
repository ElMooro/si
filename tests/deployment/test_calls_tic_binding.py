from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_actual_tic_calls_compiler_storage_auditor_and_privacy():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_calls_tic_binding.py')],cwd=ROOT,capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stdout+result.stderr
