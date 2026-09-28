"""Synthetic direct/indirect Momentum consumer boundaries; no native invocation."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_momentum_price_consumers import Tests
if __name__=='__main__':unittest.main(verbosity=2)
