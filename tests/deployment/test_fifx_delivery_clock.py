"""Acceptance clocks must follow the real producer's collection order."""
from pathlib import Path
import subprocess,sys
def test_fifx_delivery_chronology_with_real_producer_order():
    root=Path(__file__).resolve().parents[2]
    subprocess.run([sys.executable,str(root/'tests/test_fifx_delivery_clock.py')],cwd=root,check=True)
