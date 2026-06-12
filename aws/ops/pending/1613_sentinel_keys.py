# ops 1613 — redeploy alert-sentinel v1.0.1, re-seed, verify phase+gross populate
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1613}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-alert-sentinel/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-alert-sentinel/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-alert-sentinel", ZipFile=buf.getvalue())
        out["fn"] = "updated"; break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-alert-sentinel")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
# re-seed: clear state so first-run path recomputes snapshot with new keys (no dup spam beyond seed)
try:
    s3.delete_object(Bucket=B, Key="data/_alerts/last.json")
except Exception:
    pass
r = lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
d = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
snap = d.get("snapshot") or {}
out["verify"] = {"version": d.get("version"), "message_sent": d.get("message_sent"),
                  "altseason_phase": snap.get("altseason_phase"),
                  "sizing_top": snap.get("sizing_top"),
                  "sizing_gross": snap.get("sizing_gross"),
                  "breadth_capwtd": snap.get("breadth_capwtd"),
                  "breadth_regime_day": snap.get("breadth_regime_day"),
                  "breakouts_n": len(snap.get("breakouts") or []),
                  "thrusts": snap.get("thrusts"),
                  "diagnostics": d.get("diagnostics")}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1613_sentinel_keys.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "phase": out["verify"]["altseason_phase"],
                   "gross": out["verify"]["sizing_gross"]}, default=str))
