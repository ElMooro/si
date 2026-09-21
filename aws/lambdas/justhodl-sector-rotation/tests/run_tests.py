from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
from test_sector_research import SectorTests,SectorStoreTests,SectorTiltTests
if __name__=='__main__':unittest.main(verbosity=2)
