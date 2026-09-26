from pathlib import Path
import subprocess, sys
root=Path(__file__).resolve().parents[4]
for name in ('test_signal_board_original_baseline.py','test_signal_board_candidate.py','test_signal_board_inventory_qualification.py','test_signal_board_native.py'):
    subprocess.run([sys.executable,str(root/'tests'/name)],cwd=root,check=True)
sys.path[:0]=[str(root/'tests'),str(root/'aws/shared'),str(root/'aws/shared/tests')]
from aaii_consumer_test_support import run as run_aaii
run_aaii()
from funding_consumer_test_support import run as run_funding
run_funding('justhodl-signal-board')
import unittest
from breadth_consumer_test_support import ConsumerTests
suite=unittest.TestLoader().discover(str(root/'aws/shared/tests'),pattern='test_inflection_authority.py')
suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerTests))
if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():raise SystemExit(1)
