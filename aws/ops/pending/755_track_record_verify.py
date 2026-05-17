"""ops/755 — verify the Opportunity Engine track-record system.

Order matters: invoke the opportunity engine first (it writes today's
snapshot), then the track-record engine (it reads the snapshots).
"""
import json, os
from datetime import datetime, timezone, date
import boto3
from botocore.config import Config

cfg = Config(read_timeout=210, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)

report = {"ops": 755, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "track-record system verify"}
today = date.today().isoformat()

# 1 ─ run the opportunity engine → writes today's snapshot
try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["engine_invoke"] = {"status": r.get("StatusCode"),
                               "fn_error": r.get("FunctionError")}
except Exception as e:
    report["engine_invoke"] = {"err": str(e)[:200]}

# 2 ─ snapshot present?
snap_ok = False
try:
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                        Key=f"data/track-record/snapshots/{today}.json")
    snap = json.loads(obj["Body"].read())
    snap_ok = isinstance(snap.get("picks"), dict) and snap.get("n", 0) > 50
    report["snapshot"] = {"date": snap.get("date"), "n_picks": snap.get("n"),
                          "sample": dict(list(snap.get("picks", {}).items())[:2])}
except Exception as e:
    report["snapshot"] = {"err": str(e)[:200]}

# 3 ─ run the track-record engine
tr_exists = True
try:
    r = lam.invoke(FunctionName="justhodl-track-record",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["track_invoke"] = {"status": r.get("StatusCode"),
                              "fn_error": r.get("FunctionError"),
                              "body": (r["Payload"].read().decode()[:300]
                                       if r.get("Payload") else "")}
except lam.exceptions.ResourceNotFoundException:
    tr_exists = False
    report["track_invoke"] = {"err": "Lambda not found — deploy not yet landed"}
except Exception as e:
    report["track_invoke"] = {"err": str(e)[:200]}

# 4 ─ track-record output
tr = None
try:
    tr = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/track-record.json")["Body"].read())
    report["track_output"] = {
        "schema_version": tr.get("schema_version"),
        "n_snapshots": tr.get("n_snapshots"),
        "inception_date": tr.get("inception_date"),
        "days_logged": tr.get("days_logged"),
        "headline": tr.get("headline"),
        "horizon_keys": list((tr.get("horizons") or {}).keys()),
        "has_methodology": bool(tr.get("methodology")),
        "has_disclaimer": bool(tr.get("disclaimer")),
    }
except Exception as e:
    report["track_output"] = {"err": str(e)[:200]}

# 5 ─ EventBridge schedule
try:
    rule = ev.describe_rule(Name="track-record-daily")
    report["schedule"] = {"state": rule.get("State"),
                          "cron": rule.get("ScheduleExpression")}
except Exception as e:
    report["schedule"] = {"err": str(e)[:200]}

# 6 ─ page
try:
    import urllib.request
    req = urllib.request.Request("https://justhodl.ai/opportunities.html",
                                 headers={"User-Agent": "justhodl-ops/755"})
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "replace")
    page_panel = ("Track Record" in html and "renderTrackRecord" in html
                  and "track-record.json" in html)
except Exception as e:
    html, page_panel = "", False
    report["page_err"] = str(e)[:200]

checks = {
    "engine_invoke_ok": report.get("engine_invoke", {}).get("status") == 200
                        and not report.get("engine_invoke", {}).get("fn_error"),
    "snapshot_written_today": snap_ok,
    "track_record_lambda_deployed": tr_exists
        and report.get("track_invoke", {}).get("status") == 200
        and not report.get("track_invoke", {}).get("fn_error"),
    "track_record_output_ok": bool(tr) and tr.get("schema_version") == "1.0"
        and bool(tr.get("horizons")) and bool(tr.get("headline")),
    "schedule_rule_exists": "cron" in report.get("schedule", {}),
    "page_track_panel_live": page_panel,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "TRACK-RECORD SYSTEM LIVE — daily snapshot ledger writing, track-record "
    "engine computing tier returns/alpha, schedule + page panel verified. "
    "Performance fills in as snapshots mature (first 30-day results in ~30 days)."
    if report["all_pass"]
    else "REVIEW — see checks[] (new Lambda creation can lag one deploy cycle)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/755_track_record_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/755_track_record_verify.json")
