"""Synthetic source-context and preserved classifier regressions only."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_catalyst_classifier_original_validation import Tests as OriginalTests
from test_catalyst_context import Tests as CandidateTests
if __name__=='__main__':unittest.main(verbosity=2)
