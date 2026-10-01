
if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from insider_consumer_test_support import run as run_insider_consumers
    run_insider_consumers()

if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tests"))
    from sec_consumer_test_support import run as run_sec_research
    run_sec_research()

if __name__=='__main__':
    from pathlib import Path
    import sys,subprocess
    root=Path(__file__).resolve().parents[4]
    subprocess.run([sys.executable,str(root/'tests/test_option_scanner_boundary.py')],check=True)
