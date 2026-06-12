# ops 1614 — crisis-canaries v3.0: redeploy, timeout bump, invoke, verify all 31 globals
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1614}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-crisis-canaries/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-crisis-canaries/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-crisis-canaries", ZipFile=buf.getvalue())
        out["fn"] = "updated"; break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
def ready():
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName="justhodl-crisis-canaries")
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return c
        time.sleep(3)
c = ready()
if (c.get("Timeout") or 0) < 300 or (c.get("MemorySize") or 0) < 512:
    lam.update_function_configuration(FunctionName="justhodl-crisis-canaries",
                                       Timeout=300, MemorySize=512)
    ready()
    out["cfg"] = "timeout/mem bumped to 300/512"
r = lam.invoke(FunctionName="justhodl-crisis-canaries", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-1800:]
d = json.loads(s3.get_object(Bucket=B, Key="data/crisis-canaries.json")["Body"].read())
GLOBALS = ["sahm_rule","claims_4wk","continuing_claims","quits_rate","overtime_mfg",
            "heavy_trucks","building_permits","truck_tonnage","rail_freight","wei",
            "ccc_vs_bb","ci_loans","bank_credit","sloos","cc_delinquency",
            "uninversion_trigger","inversion_breadth","breakevens","twoyr_collapse",
            "copper_gold","aud_jpy","usd_clp","em_fx_stress","chile_ip",
            "transports_rel","defensives_bid","regional_banks_rel","korea_beta",
            "swap_lines","foreign_repo_pool","real_m2"]
can = d.get("canaries") or {}
avail = d.get("availability") or {}
got, miss = {}, []
for k in GLOBALS:
    v = can.get(k)
    if isinstance(v, dict) and "signal" in v:
        got[k] = {kk: v.get(kk) for kk in ("name","value","unit","signal","status",
                                             "detail","as_of","z","lead","source")}
    else:
        miss.append({"key": k, "avail": avail.get(k)})
out["verify"] = {"version": d.get("version"), "duration_s": d.get("duration_s"),
                  "n_got": len(got), "n_miss": len(miss), "missing": miss,
                  "families": d.get("families"), "red_count": d.get("red_count"),
                  "n_global": d.get("n_global"),
                  "plumbing_score": d.get("composite_score"),
                  "global_score": d.get("global_score"),
                  "grid_score_ingested": d.get("grid_score_ingested"),
                  "composite_v3": d.get("composite_v3"), "level_v3": d.get("level_v3"),
                  "alerts": d.get("alerts"), "canaries": got}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1614_canaries_v3.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "got": len(got), "miss": len(miss),
                   "v3": d.get("composite_v3"), "level": d.get("level_v3")}, default=str))
