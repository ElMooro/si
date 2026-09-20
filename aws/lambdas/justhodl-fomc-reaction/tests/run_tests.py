from pathlib import Path
import subprocess,sys,unittest
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(HERE),pattern='test_*.py'))
    if not result.wasSuccessful():raise SystemExit(1)
    for name in ('fedwatch_consumer_test_support.py','fomc_consumer_test_support.py'):
        subprocess.run([sys.executable,str(ROOT/'tests'/name)],check=True)
