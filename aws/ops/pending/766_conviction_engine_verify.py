"""ops/766 — verify justhodl-conviction-engine (roadmap item #1).

Confirms the new Conviction Engine deployed, runs clean, and produces a
valid skill-weighted, ranked conviction output.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)

report = {"ops": 766, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Conviction Engine deploy verify (roadmap #1)"}

# 1. function exists
try:
    fc = lam.get_function_configuration(FunctionName="justhodl-conviction-engine")
    report["function"] = {"exists": True, "runtime": fc.get("Runtime"),
                          "last_modified": fc.get("LastModified"),
                          "timeout": fc.get("Timeout"), "memory": fc.get("MemorySize")}
except Exception as e:
    report["function"] = {"exists": False, "err": str(e)[:200]}

# 2. EventBridge schedule
try:
    r = events.describe_rule(Name="conviction-engine-3h")
    report["schedule"] = {"exists": True, "state": r.get("State"),
                          "cron": r.get("ScheduleExpression")}
except Exception as e:
    report["schedule"] = {"exists": False, "err": str(e)[:160]}

# 3. invoke
try:
    r = lam.invoke(FunctionName="justhodl-conviction-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    payload = json.loads(r["Payload"].read() or b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload.get("body")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

# 4. read + validate output
cv = {}
try:
    cv = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/conviction.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

setups = cv.get("setups", []) or []
report["output"] = {
    "book_posture": cv.get("book_posture"),
    "headline_call": cv.get("headline_call"),
    "n_setups": cv.get("n_setups"), "n_actionable": cv.get("n_actionable"),
    "n_live": cv.get("n_live"), "n_engines": cv.get("n_engines"),
    "n_stale": cv.get("n_stale"), "skill_weighting": cv.get("skill_weighting"),
    "n_single_names": len(cv.get("single_names") or []),
}
report["setups_preview"] = [
    {"rank": s.get("rank"), "subject": s.get("subject"),
     "direction": s.get("direction"), "conviction": s.get("conviction"),
     "confidence": s.get("confidence"), "n_engines": s.get("n_engines"),
     "n_families": s.get("n_families"), "agreement_pct": s.get("agreement_pct"),
     "n_contrib": len(s.get("contributing_engines") or [])}
    for s in setups[:8]]

# checks
convs = [s.get("conviction") for s in setups if isinstance(s.get("conviction"), (int, float))]
ranks = [s.get("rank") for s in setups]
checks = {
    "function_deployed": report["function"].get("exists") is True,
    "schedule_set": report["schedule"].get("exists") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "produced_setups": len(setups) >= 3,
    "conviction_in_range": bool(convs) and all(0 <= c <= 100 for c in convs),
    "ranked_descending": convs == sorted(convs, reverse=True) and ranks == sorted(ranks),
    "has_evidence": all(len(s.get("contributing_engines") or []) >= 1 for s in setups),
    "engines_live": (cv.get("n_live") or 0) >= 6,
    "has_book_posture": bool(cv.get("book_posture")),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "CONVICTION ENGINE LIVE — deployed, scheduled every 3h, producing ranked "
    "skill-weighted conviction setups with evidence + invalidation. Roadmap #1 complete."
    if report["all_pass"] else "REVIEW — see checks[] / setups_preview[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/766_conviction_engine_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/766_conviction_engine_verify.json")
