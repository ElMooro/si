from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/"tests"))
from nowcast_consumer_test_support import Boundaries
if __name__=="__main__":unittest.main(verbosity=2)
