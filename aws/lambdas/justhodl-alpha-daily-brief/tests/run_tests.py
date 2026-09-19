"""Run Alpha replay and active no-paid/no-notification contract regressions."""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'shared/tests'))
from alpha_research_tests import run
run()
