"""ops/777 — archive orphaned legacy data outputs + re-verify fleet-monitor.

Triage (ops 776) found no broken engines — but 4 outputs are orphaned legacy
files from deprecated features (no producer anywhere, no live page). This
moves them to data/_archive/ (reversible) so the health signal is honest,
redeploys the tuned fleet-monitor, and confirms the system reads clean.
"""
import json, os, io, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-fleet-monitor"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
ORPHANS = ["dex-scanner-data", "skew", "institutional-convergence",
           "pre-pump-calibration"]

report = {"ops": 777, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Archive orphaned legacy outputs + re-verify fleet-monitor"}

# 1. archive orphans  data/X.json -> data/_archive/X.json  (copy then delete)
archived, archive_errs = [], []
for name in ORPHANS:
    src_key = f"data/{name}.json"
    arch_key = f"data/_archive/{name}.json"
    try:
        s3.head_object(Bucket=BUCKET, Key=src_key)
    except Exception:
        archive_errs.append(f"{name}: not present (already gone)")
        continue
    try:
        s3.copy_object(Bucket=BUCKET, Key=arch_key,
                       CopySource={"Bucket": BUCKET, "Key": src_key},
                       MetadataDirective="COPY")
        s3.delete_object(Bucket=BUCKET, Key=src_key)
        archived.append(name)
    except Exception as e:
        archive_errs.append(f"{name}: {str(e)[:120]}")
report["archived"] = archived
report["archive_errors"] = archive_errs

# 2. redeploy the tuned fleet-monitor from repo source
try:
    code = open(SRC, encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    report["redeploy"] = "ok"
    report["exemption_in_code"] = ("STATIC_OUTPUTS" in code
                                   and 'startswith("user-")' in code)
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

# 3. re-invoke and read the cleaned-up fleet state
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
report["remaining_red"] = [{"output": x.get("output"),
                            "age_hours": x.get("age_hours"),
                            "issue": x.get("issue")}
                           for x in (do.get("red") or [])]
report["n_static_exempted"] = do.get("n_static")
report["dependencies"] = [{"name": p.get("name"), "status": p.get("status")}
                          for p in (fl.get("dependencies") or [])]

checks = {
    "orphans_archived": len(archived) >= 3,
    "redeploy_ok": report.get("redeploy") == "ok",
    "exemption_live": report.get("exemption_in_code") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "red_cleared": len(do.get("red") or []) == 0,
    "deps_all_green": all(p.get("status") == "green"
                          for p in (fl.get("dependencies") or []))
                      and len(fl.get("dependencies") or []) >= 6,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CLEANUP COMPLETE — {len(archived)} orphaned legacy outputs archived to "
    "data/_archive/, fleet-monitor tuned (user/static outputs exempt). "
    f"System status now {fl.get('system_status')}; RED reflects only genuine "
    "scheduled-engine decay. No broken engines existed — the platform's "
    "compute and dependency layers are fully healthy."
    if report["all_pass"] else
    f"REVIEW — system={fl.get('system_status')}, see checks[]/remaining_red")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/777_orphan_cleanup_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/777_orphan_cleanup_verify.json")
