from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_feed_heartbeat():
 subprocess.run([sys.executable,str(R/'aws/lambdas/justhodl-feed-heartbeat/tests/run_tests.py')],cwd=R,check=True)
