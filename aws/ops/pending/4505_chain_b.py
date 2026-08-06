"""ops 4505 — orphan-chain B: GitHub 'Set up job' outage (15:22-16:50)
killed five runs; chain-A proved in-process runpy heals them. Final trio:
canary-macro create+run, residuals batch, E10 walker create+first-walk."""
import os,runpy,traceback,json
from datetime import datetime,timezone
R={"ops":4505,"started":datetime.now(timezone.utc).isoformat(),"ran":{}}
for f in ["4501_canary.py","4502_fixes_batch.py","4503_walker.py"]:
    p=os.path.join("aws/ops/pending",f)
    try:
        runpy.run_path(p,run_name="__main__")
        R["ran"][f]="OK"
    except SystemExit as e:
        R["ran"][f]=f"exit {e.code}"
    except Exception as e:
        R["ran"][f]=f"{type(e).__name__}: {str(e)[:140]}"
        traceback.print_exc()
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4505_chain_b.json","w"),indent=1)
open("aws/ops/reports/4505_chain_b.md","w").write("# ops 4505 chain B — "+json.dumps(R["ran"])+"\n")
print(json.dumps(R["ran"]))
