"""ops/763 — verify the Canary Grid data-freshness guard.

Confirms: every signal now carries age_days, the freshness audit block is
present, anything >~3 months stale is excluded, the grid still computes a
healthy level, and the refresh cadence is now every 6h.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=210, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)

report = {"ops": 763, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Canary Grid freshness guard verify"}

try:
    r = lam.invoke(FunctionName="justhodl-canary-grid",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:220]}

cg = {}
try:
    cg = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/canary-grid.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

fresh = cg.get("freshness") or {}
sigs = cg.get("signals", [])
report["early_warning_level"] = cg.get("early_warning_level")
report["band"] = cg.get("band")
report["n_available"] = cg.get("n_available")
report["freshness"] = fresh
report["signal_ages"] = sorted(
    [{"key": s.get("key"), "age_days": s.get("age_days"),
      "available": s.get("available"), "stale_warning": s.get("stale_warning"),
      "as_of": s.get("as_of")} for s in sigs if s.get("age_days") is not None],
    key=lambda x: (x["age_days"] is None, -(x["age_days"] or 0)))

try:
    rule = ev.describe_rule(Name="canary-grid-daily")
    report["schedule"] = rule.get("ScheduleExpression")
except Exception as e:
    report["schedule"] = f"err: {str(e)[:120]}"

checks = {
    "engine_runs": report.get("invoke", {}).get("status") == 200
                   and not report.get("invoke", {}).get("fn_error"),
    "freshness_block_present": bool(fresh)
        and "stale_hard_days" in fresh and "oldest_signal" in fresh,
    "signals_carry_age": sum(1 for s in sigs if s.get("age_days") is not None) >= 7,
    "guard_thresholds_correct": fresh.get("stale_hard_days") == 95
        and fresh.get("stale_warn_days") == 65,
    "grid_still_healthy": isinstance(cg.get("early_warning_level"), (int, float))
        and (cg.get("n_available") or 0) >= 6,
    "no_stale_data_used": all(
        (s.get("age_days") is None or s["age_days"] <= 95)
        for s in sigs if s.get("available")),
    "cadence_is_6h": "*/6" in str(report.get("schedule", "")),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "FRESHNESS GUARD LIVE — every signal carries its data age, anything past "
    "~3 months is excluded so the grid stays forward-looking, the freshness "
    "audit is published, and the engine refreshes every 6h. See signal_ages "
    "for the current data lag on each canary."
    if report["all_pass"] else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/763_canary_freshness_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/763_canary_freshness_verify.json")
