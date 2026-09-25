"""Reviewed 429 recovery: reuse the successful canary; one request/second.

The failed attempt stays archived. After complete source/privacy verification,
a conditional control update permits later disjoint batches to continue.
"""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
from share_structure_batch_runner import main
if __name__=='__main__':
    try:main(2,recovery=True)
    except Exception:sys.exit(1)
