# ops 1621 — map v1.2.1 theme-universe P/S + insider ps>0 guard: redeploy, invoke, verify
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1621}
def zipdir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(d):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()
def upd(fn, zb):
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return
        time.sleep(3)
upd("justhodl-market-map", zipdir("aws/lambdas/justhodl-market-map/source"))
r = lam.invoke(FunctionName="justhodl-market-map", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["map_err"] = r.get("FunctionError", "NONE")
g3 = json.loads(s3.get_object(Bucket=B, Key="data/themes.json")["Body"].read())
out["themes_ps"] = [{"theme": t.get("theme"), "ps_median": t.get("ps_median"),
                      "member_ps": {m["t"]: m.get("ps") for m in (t.get("members") or [])[:6]}}
                     for t in (g3.get("themes") or [])]
d = json.loads(s3.get_object(Bucket=B, Key="data/market-map.json")["Body"].read())
out["map_diag"] = d.get("diagnostics")
out["cheap_head3"] = (d.get("cheap_candidates") or [])[:3]
upd("justhodl-insider-radar", zipdir("aws/lambdas/justhodl-insider-radar/source"))
r2 = lam.invoke(FunctionName="justhodl-insider-radar", InvocationType="RequestResponse", Payload=b"{}")
out["ins_err"] = r2.get("FunctionError", "NONE")
di = json.loads(s3.get_object(Bucket=B, Key="data/insider-radar.json")["Body"].read())
out["clusters_ps"] = [{"t": c.get("ticker"), "ps": c.get("ps_ttm")} for c in (di.get("clusters") or [])[:6]]
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1621_ps_polish.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"map_err": out["map_err"], "ins_err": out["ins_err"]}, default=str))
