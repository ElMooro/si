"""ops/719 — verify the signal-scorecard directional-only grading fix.

Report 718 found signal-scorecard grading 27/43 signals DEPRECATED at a
12.8% avg Wilson LB — a measurement artifact from NEUTRAL-defaulted outcomes
being scored as misses. commit 6643255 fixes it: grading now runs on
DIRECTIONAL outcomes only.

This script invokes the redeployed Lambda, reads data/signal-scorecard.json,
and checks the numbers are now plausible:
  - schema_version == 2.0  (confirms the new code is live)
  - avg_graded_wilson_lb is sane (artifact would keep it ~0.13)
  - deprecation count is sensible, not 27/43
  - n_neutral is reported separately and is non-trivial (proves the split)
"""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 719, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "signal-scorecard directional-only grading fix"}

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
try:
    o = s3.get_object(Bucket=BUCKET, Key="data/signal-scorecard.json")
    d = json.loads(o["Body"].read())
    last_mod = o["LastModified"].isoformat()
except Exception as e:
    report["sidecar"] = {"_error": str(e)[:200]}
    print(json.dumps(report, indent=2))
    raise SystemExit(0)

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

# 3 — top 6 and bottom 6 graded signals for eyeballing
def row(r):
    return {"signal": r.get("signal_type"), "grade": r.get("grade"),
            "status": r.get("status"), "n_dir": r.get("n_directional"),
            "n_neutral": r.get("n_neutral"), "hit_rate": r.get("hit_rate"),
            "wilson_lb": r.get("wilson_lb"), "mult": r.get("performance_multiplier")}

report["top_graded"] = [row(r) for r in graded[:6]]
report["bottom_graded"] = [row(r) for r in graded[-6:]]

# 4 — verdict checks
lb = d.get("avg_graded_wilson_lb")
checks = {
    "schema_is_v2": d.get("schema_version") == "2.0",
    "method_is_directional": "directional" in (d.get("method") or ""),
    "avg_lb_no_longer_artifact": (lb is not None and lb >= 0.35),
    "deprecation_sensible": (d.get("n_deprecated", 99) <= max(3, len(graded) // 3)),
    "neutral_bucketed_separately": (d.get("n_outcomes_neutral", 0) or 0) > 0,
    "directional_outcomes_present": (d.get("n_outcomes_directional", 0) or 0) > 0,
    "graded_set_nonempty": len(graded) > 0,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = ("FIX VERIFIED — scorecard now grades on directional "
                      "outcomes only" if report["all_pass"]
                      else "REVIEW — one or more checks failed")

print(json.dumps(report, indent=2))

# 5 — persist
try:
    s3.put_object(Bucket=BUCKET, Key="_ops/reports/719_scorecard_fix_verify.json",
                   Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
                   ContentType="application/json")
except Exception as e:
    print(f"[warn] could not write S3 report copy: {e}")
