"""Synthetic acquisition, reproducibility and preserved original regressions."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_pump_positioning_original import Tests as OriginalTests
from test_positioning_observations import Tests as ObservationTests
from test_positioning_writer import Tests as WriterTests
if __name__=='__main__':unittest.main(verbosity=2)
