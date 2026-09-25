"""Capture part 2 of six disjoint complete-population source batches."""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
from share_structure_batch_runner import main

if __name__=='__main__':
    try:main(2)
    except Exception:sys.exit(1)
