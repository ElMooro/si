if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from credit_consumer_test_support import run as run_credit_consumers
    run_credit_consumers()
    from volatility_consumer_test_support import run as run_volatility_consumers
    run_volatility_consumers()

if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/"tests"))
    from eurodollar_consumer_test_support import run as run_eurodollar_consumers
    run_eurodollar_consumers()

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from insider_consumer_test_support import run as run_insider_consumers
    run_insider_consumers()

if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from extremes_consumer_test_support import run as run_extremes_consumers
    run_extremes_consumers()
    from extremes_native_test_support import run as run_extremes_native
    run_extremes_native()

if __name__=='__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from retail_consumer_test_support import run as run_retail
    run_retail()
