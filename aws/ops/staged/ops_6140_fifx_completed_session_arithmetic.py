"""Qualify retained FI/FX arithmetic with explicit completed-session evidence.

All source bytes are reused. No provider request, native invocation, current
packet, schedule, notification or portfolio mutation is performed.
"""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
from ops_6138_fifx_retained_arithmetic import main

if __name__=='__main__':
    try:main(request_id='chatgpt-fifx-completed-session-arithmetic-6140',
             report_name='ops_6140_fifx_completed_session_arithmetic')
    except Exception:sys.exit(1)
