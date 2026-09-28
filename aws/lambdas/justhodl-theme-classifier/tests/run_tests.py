"""Synthetic original defects, issuer observations, native replay and output ownership."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'tests/ops')]
from test_theme_classifier_original import Tests as OriginalTests
from test_profile_observations import Tests as ObservationTests
from test_profile_writer import Tests as WriterTests
from test_theme_output_ownership import ThemeOutputOwnershipTests
if __name__=='__main__':unittest.main(verbosity=2)
