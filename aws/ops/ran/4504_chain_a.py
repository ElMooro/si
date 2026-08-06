"""ops 4504 — orphan-chain A: the runner executes newest-push only, so
rapid pushes orphaned 4497/4499/4500. Run their bodies in-process,
sequentially, isolated; each writes its own report."""
import os,runpy,traceback,json
from datetime import datetime,timezone
R={"ops":4504,"started":datetime.now(timezone.utc).isoformat(),"ran":{}}
for f in ["4499_read_reasons.py","4500_p0_resurrect.py","4497_panel2.py"]:
    p=os.path.join("aws/ops/pending",f)
    try:
        runpy.run_path(p,run_name="__main__")
        R["ran"][f]="OK"
    except SystemExit as e:
        R["ran"][f]=f"exit {e.code}"
    except Exception as e:
        R["ran"][f]=f"{type(e).__name__}: {str(e)[:120]}"
        traceback.print_exc()
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4504_chain_a.json","w"),indent=1)
open("aws/ops/reports/4504_chain_a.md","w").write("# ops 4504 chain A — "+json.dumps(R["ran"])+"\n")
print(json.dumps(R["ran"]))
