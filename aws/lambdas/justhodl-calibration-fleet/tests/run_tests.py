
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
    from extremes_consumer_test_support import run as run_extremes_consumers
    run_extremes_consumers()

if __name__ == '__main__':
    import subprocess
    ROOT = Path(__file__).resolve().parents[4]
    for suite in ('test_signal_board_stream_and_calibration.py', 'test_dollar_research_consumers.py'):
        subprocess.run([sys.executable, str(ROOT/'tests'/suite)], cwd=ROOT, check=True)
