from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[4]
for name in ('test_coverage_model.py','test_coverage_store.py','test_coverage_native.py'):
    subprocess.run([sys.executable,'-X','utf8',str(root/'tests'/name)],cwd=root,check=True)
