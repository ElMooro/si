"""Reviewed migration entrypoint; draft only until independent approval.
Update the existing desk rule to 23:05 UTC, with archival and exact readback.
"""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops/checks'))
from etf_desk_phase import run_operation

if __name__ == '__main__':
    try:
        run_operation(report_name=Path(__file__).stem)
    except Exception:
        sys.exit(1)
