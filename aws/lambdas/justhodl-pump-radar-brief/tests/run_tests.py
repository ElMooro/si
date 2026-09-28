"""Preserved synthesis counterexamples and synthetic deterministic brief checks."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_pump_brief_original_synthesis import Tests as OriginalTests
from test_pump_brief_evidence import Tests as EvidenceTests
if __name__=='__main__':unittest.main(verbosity=2)
