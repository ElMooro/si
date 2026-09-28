"""Synthetic direct/indirect Momentum consumer boundaries; no native invocation."""
from pathlib import Path
import sys,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests/ops'))
from test_momentum_price_consumers import Tests as BoundaryTests
from test_velocity_original_observations import Tests as OriginalTests
from test_velocity_volume_observations import Tests as MeasurementTests
from test_velocity_evidence import Tests as EvidenceTests
from test_velocity_writer import Tests as WriterTests
from test_velocity_consumers import Tests as ConsumerTests
if __name__=='__main__':unittest.main(verbosity=2)
