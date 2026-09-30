"""Reviewed rollback entrypoint; requires prepared old config and retained reversal.
Do not dispatch unless independently approved; never invoke the producer.
"""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops/checks'))
from etf_desk_phase import run_operation

if __name__ == '__main__':
    try:
        run_operation(rollback=True, report_name=Path(__file__).stem)
    except Exception:
        sys.exit(1)
