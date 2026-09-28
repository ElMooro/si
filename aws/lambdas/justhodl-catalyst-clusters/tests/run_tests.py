"""Native cluster abstention and predecessor regressions, synthetic only."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_catalyst_cluster_original_grades import Tests as OriginalTests
from test_catalyst_cluster_abstention import Tests as CandidateTests
if __name__=='__main__':unittest.main(verbosity=2)
