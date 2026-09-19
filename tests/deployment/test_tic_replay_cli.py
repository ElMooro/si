"""The public TIC reproduction command bootstraps outside the ops environment."""
import os,subprocess,sys,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]


def test_tic_replay_cli_starts_without_pythonpath_from_an_unrelated_directory():
    env=os.environ.copy();env.pop('PYTHONPATH',None);env['PYTHONUTF8']='1'
    with tempfile.TemporaryDirectory() as directory:
        result=subprocess.run([sys.executable,str(ROOT/'scripts/replay_tic_research.py'),'--help'],cwd=directory,
            env=env,text=True,capture_output=True,timeout=30)
    assert result.returncode==0,result.stderr
    assert '--run' in result.stdout
