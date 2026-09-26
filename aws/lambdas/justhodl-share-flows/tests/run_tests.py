"""Native publication, exact arithmetic, replay, and downstream authority checks."""
from pathlib import Path
import subprocess, sys, unittest
ROOT=Path(__file__).resolve().parents[4]
HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks'),str(ROOT/'tests'),str(HERE)]
for name in ('measurements','research','store','arithmetic','producer','readiness','context','consumers',
             'remaining_consumers','final_consumers'):
    path=ROOT/'tests'/('test_capital_structure_'+name+'.py')
    subprocess.run([sys.executable,str(path)],cwd=ROOT,check=True)
suite=unittest.TestLoader().discover(str(HERE),pattern='test_native.py')
result=unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
