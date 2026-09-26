from pathlib import Path
import subprocess, sys
ROOT = Path(__file__).resolve().parents[4]
if __name__ == '__main__':
    from pathlib import Path
    import subprocess, sys
    subprocess.run([sys.executable,str(Path(__file__).resolve().parents[4]/'tests/test_master_allocation_authority.py')],check=True)

if __name__ == '__main__':
    for suite in ('test_signal_board_stream_and_calibration.py', 'test_dollar_research_consumers.py'):
        subprocess.run([sys.executable, str(ROOT/'tests'/suite)], cwd=ROOT, check=True)
