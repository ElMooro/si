from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/"aws/shared"),str(ROOT/"aws/shared/tests")]
from reversal_consumer_tests import run
run('physical-econ')
