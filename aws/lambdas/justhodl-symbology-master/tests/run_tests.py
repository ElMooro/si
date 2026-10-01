from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[4]
for name in ('test_symbology_integrity.py','test_equity_identity.py'):
    subprocess.run([sys.executable,'-X','utf8',str(root/'tests'/name)],cwd=root,check=True)
