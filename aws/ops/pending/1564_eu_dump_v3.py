# ops 1564 — ecb-derived v3.0: inject ANTHROPIC_API_KEY from justhodl-ai-brief env,
# raise timeout/mem, deploy, invoke, verify charts/event-study/AI-brief/score
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1564, "errors": []}

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

# A) env merge: copy ANTHROPIC_API_KEY from ai-brief
src_env = lam.get_function_configuration(FunctionName="justhodl-ai-brief")["Environment"]["Variables"]
akey = src_env.get("ANTHROPIC_API_KEY","")
out["akey_found"] = bool(akey)
cur = lam.get_function_configuration(FunctionName="justhodl-ecb-derived")
env = dict((cur.get("Environment") or {}).get("Variables") or {})
env["ANTHROPIC_API_KEY"] = akey
rc(lambda: lam.update_function_configuration(FunctionName="justhodl-ecb-derived",
    Environment={"Variables": env}, Timeout=600, MemorySize=1024))
ready("justhodl-ecb-derived")

# B) deploy v3.0 + invoke
rc(lambda: lam.update_function_code(FunctionName="justhodl-ecb-derived",
    ZipFile=zs("aws/lambdas/justhodl-ecb-derived/source")))
ready("justhodl-ecb-derived")
r = rc(lambda: lam.invoke(FunctionName="justhodl-ecb-derived", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"] = r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)

# C) verify
d = json.loads(s3.get_object(Bucket=B, Key="data/ecb-derived.json")["Body"].read())
ch = d.get("charts") or {}
out["verify"] = {
  "version": d.get("version"), "duration_s": d.get("duration_s"),
  "brief_kb": round(len(json.dumps(d, default=str))/1024, 1),
  "charts": {k: {"n": len(v.get("points") or []), "pctile": v.get("pctile"),
                  "latest": v.get("latest"), "since": v.get("first_date")} for k, v in ch.items()},
  "event_study": {kk: d.get("event_study", {}).get(kk) for kk in
                   ("n_episodes","spx","eurusd","err","definition")},
  "episode_dates_tail": (d.get("event_study", {}).get("episode_dates") or [])[-8:],
  "dump_score": d.get("dump_score"),
  "n_flashing": d.get("n_flashing"), "flashing": d.get("flashing"),
  "ai_brief_keys": sorted((d.get("ai_brief") or {}).keys()),
  "ai_error": (d.get("ai_brief") or {}).get("error"),
  "ai_base_case": str((d.get("ai_brief") or {}).get("base_case",""))[:300],
  "ai_watch_next": (d.get("ai_brief") or {}).get("watch_next"),
  "signal_logged": d.get("signal_logged", False)}
open("aws/ops/reports/1564_eu_dump_v3.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps(out["verify"],default=str)[:1400])
