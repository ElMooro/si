from pathlib import Path
import subprocess, sys
HERE = Path(__file__).resolve().parent
subprocess.run([sys.executable, str(HERE/'test_liquidity_native.py')], check=True)

subprocess.run([sys.executable, str(HERE.parents[3]/'tests/liquidity_transport_tests.py')], check=True)
