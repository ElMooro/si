"""Recheck Dollar after the edge cache repair; existing publication only."""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops/staged')]
from ops_6025_dollar_native_acceptance import main

if __name__=='__main__':
    try:main(report_name='ops_6027_dollar_edge_acceptance',require_existing=True)
    except Exception:sys.exit(1)
