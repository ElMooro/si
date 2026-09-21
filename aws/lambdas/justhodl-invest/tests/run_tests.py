from pathlib import Path
import subprocess,sys
HERE=Path(__file__).resolve().parent
if __name__=='__main__':sys.exit(subprocess.call([sys.executable,'-m','pytest','-q',str(HERE)]))

if __name__ == '__main__':
    import sys,unittest
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from sector_consumer_test_support import SectorBoundaries
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SectorBoundaries))
    if not result.wasSuccessful():raise SystemExit(1)
