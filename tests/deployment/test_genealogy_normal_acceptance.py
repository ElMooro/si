from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_original_cadence_genealogy_acceptance_is_full_replay_and_read_only():
    result=subprocess.run([sys.executable,str(ROOT/'tests/ops/test_genealogy_normal_acceptance.py')],cwd=ROOT,capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr
