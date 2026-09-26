from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[4]
for name in ('test_ici_original_baseline.py','test_ici_research_candidate.py','test_ici_native.py'):
    subprocess.run([sys.executable,str(root/'tests'/name)],cwd=root,check=True)
