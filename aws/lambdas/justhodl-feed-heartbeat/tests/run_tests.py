from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[4]
for name in ('test_feed_heartbeat_predecessor.py','test_feed_heartbeat.py'):
 subprocess.run([sys.executable,str(R/'tests'/name)],cwd=R,check=True)
