# ops 1566 — v3.0.2 AI-brief fix (tokens + salvage parser); deploy, invoke, final verify
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1566, "errors": []}

def rc(fn, tries=10, wait=8):
    for i in range(tries):
        try: return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException","TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries")

def zs(src):
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r: zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
    return b.getvalue()

def ready(fn):
    for _ in range(50):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

rc(lambda: lam.update_function_code(FunctionName="justhodl-ecb-derived",
    ZipFile=zs("aws/lambdas/justhodl-ecb-derived/source")))
ready("justhodl-ecb-derived")
r = rc(lambda: lam.invoke(FunctionName="justhodl-ecb-derived", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"] = r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:500]
time.sleep(2)
d = json.loads(s3.get_object(Bucket=B, Key="data/ecb-derived.json")["Body"].read())
ai = d.get("ai_brief") or {}
out["verify"] = {
  "version": d.get("version"), "duration_s": d.get("duration_s"),
  "brief_kb": round(len(json.dumps(d, default=str))/1024, 1),
  "charts_n": {k: len(v.get("points") or []) for k, v in (d.get("charts") or {}).items()},
  "dump_score": (d.get("dump_score") or {}).get("score_0_100"),
  "ai_error": ai.get("error"), "ai_keys": sorted(ai.keys()),
  "ai_what": str(ai.get("what_this_is",""))[:250],
  "ai_why": str(ai.get("why_it_matters",""))[:250],
  "ai_cvh": str(ai.get("current_vs_history",""))[:300],
  "ai_base_case": str(ai.get("base_case",""))[:400],
  "ai_tx": {k: str(v_)[:160] for k, v_ in (ai.get("transmission") or {}).items()},
  "ai_watch_next": ai.get("watch_next"),
  "ai_conf": ai.get("confidence_note")}
open("aws/ops/reports/1566_eu_dump_ai.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn_err":out["fn_err"],"ai_error":out["verify"]["ai_error"],
  "ai_keys":out["verify"]["ai_keys"]},default=str)[:300])
