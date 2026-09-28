
if __name__=='__main__':
    from pathlib import Path
    import sys,subprocess
    root=Path(__file__).resolve().parents[4]
    subprocess.run([sys.executable,str(root/'tests/test_option_scanner_boundary.py')],check=True)
