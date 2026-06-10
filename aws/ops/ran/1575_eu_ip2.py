# ops 1572 — deploy ecb-derived v3.2.0 (macro canaries); verify indicators, charts, 7-pillar score, AI brief
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1575, "errors": []}

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
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d = json.loads(s3.get_object(Bucket=B, Key="data/ecb-derived.json")["Body"].read())
ind = d.get("indicators") or {}
ch = d.get("charts") or {}
ai = d.get("ai_brief") or {}
out["verify"] = {
  "version": d.get("version"), "duration_s": d.get("duration_s"),
  "brief_kb": round(len(json.dumps(d, default=str))/1024, 1),
  "macro_indicators": {k: ({kk: ind[k].get(kk) for kk in ("signal","unemployment_rate_pct",
      "chg_3m_pp","ip_yoy_pct","real_m1_growth_pct","m1_yoy_pct","hicp_yoy_pct",
      "business_conf","business_z","consumer_z","as_of","series","err") if kk in ind[k]})
      for k in ("ea_unemployment","ea_industrial_production","ea_confidence","real_m1_growth")
      if k in ind},
  "charts": {k: {"n": len(v.get("points") or []), "pctile": v.get("pctile"),
                  "latest": v.get("latest"), "since": v.get("first_date")} for k, v in ch.items()},
  "dump_score": d.get("dump_score"),
  "n_flashing": d.get("n_flashing"), "flashing": d.get("flashing"),
  "ai_error": ai.get("error"),
  "ai_cvh": str(ai.get("current_vs_history",""))[:350],
  "ai_base": str(ai.get("base_case",""))[:300],
  "eurostat_debug": d.get("_eurostat_debug")}
open("aws/ops/reports/1575_eu_ip2.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"err":out["fn_err"],"v":out["verify"]["version"],
  "macro":out["verify"]["macro_indicators"],"score":out["verify"]["dump_score"],
  "ai_err":out["verify"]["ai_error"]},default=str)[:900])
