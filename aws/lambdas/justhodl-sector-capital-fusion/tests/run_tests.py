"""Sector input authority regressions, with no AWS calls."""

if __name__ == '__main__':
    import sys,unittest
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from sector_consumer_test_support import SectorBoundaries
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SectorBoundaries))
    if not result.wasSuccessful():raise SystemExit(1)

if __name__ == '__main__':
    from money_volume_consumer_test_support import PriceVolumeBoundaries
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(PriceVolumeBoundaries))
    if not result.wasSuccessful():raise SystemExit(1)

if __name__ == '__main__':
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'aws/shared/tests'))
    from test_sector_fusion_research import Arithmetic,Replay,Issuer
    suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(t) for t in (Arithmetic,Replay,Issuer))
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():raise SystemExit(1)
