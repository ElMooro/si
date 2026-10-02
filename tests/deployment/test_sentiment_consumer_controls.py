"""Invented read-only two-consumer baseline checks."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_sentiment_consumer_control_baseline():
 subprocess.run([sys.executable,str(R/'aws/ops/checks/test_sentiment_consumer_controls.py')],cwd=R,check=True)
