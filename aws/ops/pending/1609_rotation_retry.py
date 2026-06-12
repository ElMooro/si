# ops 1609 — redeploy rotation-radar v1.2.1 (retry+diag), SYNC invoke, verify ratios incl semis_mkt
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1609}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-rotation-radar/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-rotation-radar/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-rotation-radar", ZipFile=buf.getvalue())
        out["fn"] = "updated"; break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-rotation-radar")
    if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
        break
    time.sleep(3)
r = lam.invoke(FunctionName="justhodl-rotation-radar", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
try:
    out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-1200:]
except Exception:
    out["log_tail"] = None
d = json.loads(s3.get_object(Bucket=B, Key="data/rotation-radar.json")["Body"].read())
rr = ((d.get("equity") or {}).get("ratios")) or {}
sm = rr.get("semis_mkt") or {}
out["verify"] = {"version": d.get("version"), "generated_at": d.get("generated_at"),
                  "diagnostics": d.get("diagnostics"),
                  "ratio_keys": list(rr.keys()),
                  "semis_mkt": {"n_events": len(sm.get("events") or []),
                                 "events": sm.get("events"),
                                 "iwm_rel_stats": sm.get("iwm_rel"),
                                 "live": sm.get("live")},
                  "smallcap_live": (rr.get("smallcap_large") or {}).get("live")}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1609_rotation_retry.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "ratios": out["verify"]["ratio_keys"],
                   "semis_events": out["verify"]["semis_mkt"]["n_events"]}, default=str))
