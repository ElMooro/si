from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'shared/tests'))
from scenario_privacy_test_support import run
if __name__ == '__main__':
    raise SystemExit(0 if run('justhodl-tax-plan') else 1)
