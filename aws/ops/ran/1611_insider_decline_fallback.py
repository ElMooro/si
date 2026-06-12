# ops 1611 — redeploy insider-radar v1.0.1 + add POLYGON_KEY env; verify decline-join populates
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1611}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-insider-radar/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-insider-radar/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-insider-radar", ZipFile=buf.getvalue())
        out["fn"] = "updated"; break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-insider-radar")
    if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
        break
    time.sleep(3)
env = c.get("Environment", {}).get("Variables", {}) or {}
env["POLYGON_KEY"] = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
lam.update_function_configuration(FunctionName="justhodl-insider-radar",
                                   Environment={"Variables": env})
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-insider-radar")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
r = lam.invoke(FunctionName="justhodl-insider-radar", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-900:]
d = json.loads(s3.get_object(Bucket=B, Key="data/insider-radar.json")["Body"].read())
out["verify"] = {"version": d.get("version"), "source_used": d.get("source_used"),
                  "n_buys": d.get("n_buys"),
                  "clusters": [{"t": c.get("ticker"), "ins": c.get("n_insiders"),
                                 "val": c.get("total_value"), "ret60": c.get("ret_60d_pct")}
                                for c in (d.get("clusters") or [])[:8]],
                  "decline_clusters": d.get("decline_clusters"),
                  "decline_buys_head": [{"t": b.get("ticker"), "ret60": b.get("ret_60d_pct"),
                                          "val": b.get("value")}
                                         for b in (d.get("decline_buys") or [])[:8]],
                  "logged": d.get("logged"),
                  "diag_tail": (d.get("diagnostics") or [])[-4:]}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1611_insider_decline_fallback.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "version": out["verify"]["version"],
                   "decline_clusters": len(d.get("decline_clusters") or []),
                   "decline_buys": len(d.get("decline_buys") or [])}, default=str))
