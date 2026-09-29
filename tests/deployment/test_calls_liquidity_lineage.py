from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]

def test_complete_calls_original_liquidity_lineage_candidate():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_calls_liquidity_lineage.py')],cwd=ROOT,
        capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stdout+result.stderr
