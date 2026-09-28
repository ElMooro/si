"""Original provider-flow boundary plus isolated Cascade evidence and regression tests."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'tests/ops')]
from provider_flow_test_support import FlowBoundaries
from test_theme_cascade_original import Tests as OriginalTests
from test_cascade_evidence import Tests as EvidenceTests
from test_cascade_consumers import Tests as ConsumerTests
if __name__=='__main__':unittest.main(verbosity=2)
