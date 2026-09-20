if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from credit_consumer_test_support import run as run_credit_consumers
    run_credit_consumers()
