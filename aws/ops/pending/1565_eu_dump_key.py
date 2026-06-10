# ops 1565 — discover Anthropic key (lambda envs + SSM), inject, deploy v3.0.1, full re-verify
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1565, "errors": []}

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

# A) hunt the key
akey, found_in = "", None
cands = ["justhodl-ai-brief","justhodl-ai-chat","justhodl-morning-intel","justhodl-ab-test",
         "justhodl-ai-brief-router","justhodl-investor-agents","justhodl-daily-report-v3"]
for fn in cands:
    try:
        env = (lam.get_function_configuration(FunctionName=fn).get("Environment") or {}).get("Variables") or {}
        for k, v in env.items():
            if isinstance(v, str) and v.startswith("sk-ant-"):
                akey, found_in = v, f"{fn}:{k}"; break
        if akey: break
    except ClientError: pass
if not akey:
    try:
        pg = ssm.get_parameters_by_path(Path="/justhodl/", Recursive=True, WithDecryption=True, MaxResults=10)
        params = pg.get("Parameters", [])
        while pg.get("NextToken"):
            pg = ssm.get_parameters_by_path(Path="/justhodl/", Recursive=True, WithDecryption=True,
                                            MaxResults=10, NextToken=pg["NextToken"])
            params += pg.get("Parameters", [])
        out["ssm_names"] = [p["Name"] for p in params]
        for p in params:
            if str(p.get("Value","")).startswith("sk-ant-"):
                akey, found_in = p["Value"], f"ssm:{p['Name']}"; break
    except ClientError as e:
        out["ssm_err"] = str(e)[:80]
out["key_found_in"] = found_in

# B) inject + deploy v3.0.1 + invoke
cur = lam.get_function_configuration(FunctionName="justhodl-ecb-derived")
env = dict((cur.get("Environment") or {}).get("Variables") or {})
if akey:
    env["ANTHROPIC_API_KEY"] = akey
    rc(lambda: lam.update_function_configuration(FunctionName="justhodl-ecb-derived",
        Environment={"Variables": env}))
    ready("justhodl-ecb-derived")
rc(lambda: lam.update_function_code(FunctionName="justhodl-ecb-derived",
    ZipFile=zs("aws/lambdas/justhodl-ecb-derived/source")))
ready("justhodl-ecb-derived")
r = rc(lambda: lam.invoke(FunctionName="justhodl-ecb-derived", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"] = r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:500]
time.sleep(2)
d = json.loads(s3.get_object(Bucket=B, Key="data/ecb-derived.json")["Body"].read())
ch = d.get("charts") or {}
ai = d.get("ai_brief") or {}
out["verify"] = {
  "version": d.get("version"), "duration_s": d.get("duration_s"),
  "brief_kb": round(len(json.dumps(d, default=str))/1024, 1),
  "charts": {k: {"n": len(v.get("points") or []), "pctile": v.get("pctile")} for k, v in ch.items()},
  "empirical_read": (d.get("event_study") or {}).get("empirical_read"),
  "dump_score": d.get("dump_score"),
  "ai_keys": sorted(ai.keys()), "ai_error": ai.get("error"),
  "ai_what": str(ai.get("what_this_is",""))[:200],
  "ai_base_case": str(ai.get("base_case",""))[:350],
  "ai_transmission_keys": sorted((ai.get("transmission") or {}).keys()),
  "ai_watch_next": ai.get("watch_next")}
open("aws/ops/reports/1565_eu_dump_key.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"found_in":found_in,"fn_err":out["fn_err"],"charts":out["verify"]["charts"],
  "ai_error":out["verify"]["ai_error"],"ai_keys":out["verify"]["ai_keys"]},default=str)[:600])
