"""Official research replay starts outside its repository without runner imports."""
import os,subprocess,sys,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]

def test_official_replay_cli_starts_without_pythonpath():
    env=os.environ.copy();env.pop('PYTHONPATH',None);env['PYTHONUTF8']='1'
    with tempfile.TemporaryDirectory() as directory:
        result=subprocess.run([sys.executable,str(ROOT/'scripts/replay_official_research.py'),'--help'],cwd=directory,env=env,text=True,capture_output=True,timeout=30)
    assert result.returncode==0,result.stderr
    assert '--run' in result.stdout
