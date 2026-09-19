from pathlib import Path
import subprocess,sys
raise SystemExit(subprocess.call([sys.executable,'-m','unittest','discover','-s',str(Path(__file__).parent),'-p','test_*.py','-v']))
