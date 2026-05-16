"""ops/720 — re-run the signal-scorecard verification and write the report
INTO THE REPO (aws/ops/reports/) so it is visible without S3 access.

719 ran clean but wrote its report only to S3. Same checks here; the
Lambda is re-invoked to confirm it runs without error, then the fresh
data/signal-scorecard.json is graded against the fix criteria.
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 720, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "signal-scorecard directional-only grading fix — repo-visible verify"}

# 1 — invoke the redeployed Lambda
try:
    r = lam.invoke(FunctionName="justhodl-signal-scorecard",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "response": body[:500]}
except Exception as e:
    report["invoke"] = {"status": "error", "err": str(e)[:250]}

time.sleep(4)

# 2 — read the fresh sidecar
d, last_mod = None, None
try:
    o = s3.get_object(Bucket=BUCKET, Key="data/signal-scorecard.json")
    d = json.loads(o["Body"].read())
    last_mod = o["LastModified"].isoformat()
except Exception as e:
    report["sidecar"] = {"_error": str(e)[:200]}

if d:
    sc = d.get("scorecard", [])
    graded = [r for r in sc if r.get("n_directional", 0) >= 15]

    report["sidecar"] = {
        "last_modified": last_mod,
        "schema_version": d.get("schema_version"),
        "method": d.get("method"),
        "n_outcomes_scanned": d.get("n_outcomes_scanned"),
        "n_outcomes_directional": d.get("n_outcomes_directional"),
        "n_outcomes_neutral": d.get("n_outcomes_neutral"),
        "n_signals_tracked": d.get("n_signals_tracked"),
        "n_signals_graded": d.get("n_signals_graded"),
        "n_promoted": d.get("n_promoted"),
        "n_deprecated": d.get("n_deprecated"),
        "n_insufficient": d.get("n_insufficient"),
        "avg_graded_wilson_lb": d.get("avg_graded_wilson_lb"),
    }

    def row(r):
        return {"signal": r.get("signal_type"), "grade": r.get("grade"),
                "status": r.get("status"), "n_dir": r.get("n_directional"),
                "n_neutral": r.get("n_neutral"), "hit_rate": r.get("hit_rate"),
                "wilson_lb": r.get("wilson_lb"), "mult": r.get("performance_multiplier")}

    report["top_graded"] = [row(r) for r in graded[:8]]
    report["bottom_graded"] = [row(r) for r in graded[-8:]]
    report["deprecated_list"] = d.get("deprecated_signals", [])
    report["promoted_list"] = d.get("promoted_signals", [])

    lb = d.get("avg_graded_wilson_lb")
    checks = {
        "schema_is_v2": d.get("schema_version") == "2.0",
        "method_is_directional": "directional" in (d.get("method") or ""),
        "avg_lb_no_longer_artifact": (lb is not None and lb >= 0.35),
        "deprecation_sensible": (d.get("n_deprecated", 99) <= max(3, len(graded) // 3)),
        "neutral_bucketed_separately": (d.get("n_outcomes_neutral", 0) or 0) > 0,
        "directional_outcomes_present": (d.get("n_outcomes_directional", 0) or 0) > 0,
        "graded_set_nonempty": len(graded) > 0,
        "invoke_ok": report["invoke"].get("fn_error") is None
                     and report["invoke"].get("status") == 200,
    }
    report["checks"] = checks
    report["all_pass"] = all(checks.values())
    report["verdict"] = ("FIX VERIFIED — scorecard grades on directional outcomes only"
                         if report["all_pass"] else "REVIEW — one or more checks failed")
else:
    report["all_pass"] = False
    report["verdict"] = "REVIEW — could not read sidecar"

print(json.dumps(report, indent=2))

# 3 — write the report INTO THE REPO so run-ops auto-commits it
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/720_scorecard_fix_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/720_scorecard_fix_verify.json")
