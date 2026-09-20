
if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from insider_consumer_test_support import run as run_insider_consumers
    run_insider_consumers()
