"""Only isolated Positioning exclusion regression; no consumer/native I/O."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_positioning_consumer_guard import Tests as PositioningGuardTests
from test_apex_original_weighting import Tests as OriginalTests
from test_apex_evidence import Tests as EvidenceTests
if __name__=='__main__':unittest.main(verbosity=2)
