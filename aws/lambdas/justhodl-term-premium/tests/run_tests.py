from pathlib import Path
import subprocess,sys
HERE=Path(__file__).resolve().parent
subprocess.run([sys.executable,str(HERE/'test_term_native.py')],check=True)
