"""1294 — confirm worker /ask from real internet (GH runner)."""
import json, urllib.request
out={}
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/ask",
        data=json.dumps({"q":"top conviction setups today"}).encode(),
        headers={"Content-Type":"application/json","Origin":"https://justhodl.ai"},method="POST")
    r=urllib.request.urlopen(req,timeout=45)
    d=json.loads(r.read().decode())
    out={"status":r.status,"has_answer":bool(d.get("answer")),"n_results":len(d.get("results",[])),"answer_preview":(d.get("answer") or "")[:140]}
except urllib.error.HTTPError as e: out={"http_error":e.code,"body":e.read().decode()[:200]}
except Exception as e: out={"err":str(e)[:200]}
open("aws/ops/reports/1294_worker_ask.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
