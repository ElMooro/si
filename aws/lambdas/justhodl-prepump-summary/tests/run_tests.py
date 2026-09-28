from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_prepump_summary_original import Tests as OriginalTests
from test_prepump_summary_evidence import Tests as EvidenceTests
if __name__=='__main__':unittest.main(verbosity=2)
