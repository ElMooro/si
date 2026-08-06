import os,runpy,traceback,json
R={"ran":{}}
try:
    runpy.run_path("aws/ops/pending/4516_v5.py",run_name="__main__")
    R["ran"]["4516_v5.py"]="OK"
except SystemExit as e: R["ran"]["4516_v5.py"]=f"exit {e.code}"
except Exception as e:
    R["ran"]["4516_v5.py"]=f"{type(e).__name__}: {str(e)[:160]}"
    traceback.print_exc()
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4517_chain.md","w").write("# 4517 chain — "+json.dumps(R["ran"])+"\n")
print(json.dumps(R["ran"]))
