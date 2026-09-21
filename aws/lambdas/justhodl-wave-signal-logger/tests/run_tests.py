
if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/"tests"))
    from eurodollar_consumer_test_support import run as run_eurodollar_consumers
    run_eurodollar_consumers()

if __name__ == '__main__':
    import sys,unittest
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from sector_consumer_test_support import SectorBoundaries
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SectorBoundaries))
    if not result.wasSuccessful():raise SystemExit(1)
