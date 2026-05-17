"""ops/778 — seed cadence manifest + redeploy/verify the manifest-aware
fleet-monitor.

Builds the cadence manifest from the repo, uploads it to S3, redeploys the
fleet-monitor that reads it, and confirms outputs are now judged against
their own schedules.
"""
import json, os, sys, io, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-fleet-monitor"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"

report = {"ops": 778, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Seed cadence manifest + verify manifest-aware fleet-monitor"}

# 1. build the manifest from the repo and upload to S3
try:
    sys.path.insert(0, "aws/tools")
    from build_cadence_manifest import build_manifest
    manifest = build_manifest(".")
    s3.put_object(Bucket=BUCKET, Key="_health/cadence-manifest.json",
                  Body=json.dumps(manifest, indent=2).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="max-age=600")
    sched = sum(1 for v in manifest["outputs"].values()
                if v.get("cadence_hours") is not None)
    report["manifest"] = {"n_outputs": manifest["n_outputs"],
                          "engines_scanned": manifest["engines_scanned"],
                          "scheduled": sched,
                          "event_driven": manifest["n_outputs"] - sched}
except Exception as e:
    report["manifest_error"] = f"{type(e).__name__}: {str(e)[:240]}"

# 2. redeploy the manifest-aware fleet-monitor
try:
    code = open(SRC, encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    report["redeploy"] = "ok"
    report["manifest_code_live"] = "load_cadence_manifest" in code
except Exception as e:
    report["redeploy"] = f"{type(e).__name__}: {str(e)[:200]}"

for _ in range(30):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

# 3. invoke and read the manifest-aware sweep
fl = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    payload = json.loads(r["Payload"].read() or b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload.get("body")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:200]}
try:
    fl = json.loads(s3.get_object(Bucket=BUCKET,
                    Key="_health/fleet.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

do = fl.get("data_outputs") or {}
report["system_status"] = fl.get("system_status")
report["summary"] = fl.get("summary")
report["sweep"] = {"total": do.get("total"), "green": do.get("green"),
                   "n_yellow": do.get("n_yellow"), "n_red": do.get("n_red"),
                   "n_degraded": do.get("n_degraded"),
                   "n_static": do.get("n_static"),
                   "manifest_outputs": do.get("manifest_outputs")}
report["red"] = [{"output": x.get("output"), "age_hours": x.get("age_hours"),
                  "cadence_hours": x.get("cadence_hours"),
                  "issue": x.get("issue")} for x in (do.get("red") or [])]

mo = do.get("manifest_outputs") or 0
checks = {
    "manifest_built": report.get("manifest", {}).get("n_outputs", 0) >= 80,
    "manifest_uploaded": "manifest_error" not in report,
    "redeploy_ok": report.get("redeploy") == "ok",
    "manifest_code_live": report.get("manifest_code_live") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "monitor_using_manifest": mo >= 80,
    "system_status_set": fl.get("system_status") in ("green", "yellow", "red"),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CADENCE MANIFEST LIVE — {mo} outputs now judged against their own "
    "schedule (hourly/3h/6h/daily/weekly), not one blunt threshold. "
    "Auto-regenerates via the cadence-manifest workflow whenever any engine's "
    f"schedule changes. System status: {fl.get('system_status')}."
    if report["all_pass"] else "REVIEW — see checks[]/sweep")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/778_cadence_manifest_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/778_cadence_manifest_verify.json")
