
if __name__ == '__main__':
    from pathlib import Path
    import subprocess, sys
    subprocess.run([sys.executable,str(Path(__file__).resolve().parents[4]/'tests/test_signal_board_interpretation_consumers.py')],check=True)
