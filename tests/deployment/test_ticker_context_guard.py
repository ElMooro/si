from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_ticker_context_guard():
 subprocess.run([sys.executable,str(R/'aws/lambdas/justhodl-ticker-360/tests/run_tests.py')],cwd=R,check=True)
