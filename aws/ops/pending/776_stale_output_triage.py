"""ops/776 — stale-output triage diagnosis.

The fleet-monitor flagged 9 stale data outputs. This script gets the ground
truth: for each candidate engine — does the Lambda exist, is it scheduled,
when did it last run, is it erroring (with recent log lines)? And for the
suspected orphans, is the output still referenced by any live page?
"""
import json, os, glob, re
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
cw = boto3.client("cloudwatch", region_name="us-east-1", config=cfg)
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 776, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Stale-output triage — broken engines vs orphaned legacy"}


def metric_sum(fn, metric, days=14):
    try:
        end = datetime.now(timezone.utc)
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName=metric,
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=end - timedelta(days=days), EndTime=end,
            Period=86400, Statistics=["Sum"])
        pts = sorted(r.get("Datapoints", []), key=lambda d: d["Timestamp"])
        return {"total": round(sum(p["Sum"] for p in pts)),
                "last_day_with_activity": (pts[-1]["Timestamp"].date().isoformat()
                                           if pts else None)}
    except Exception as e:
        return {"err": str(e)[:120]}


def schedule_for(fn):
    """Find EventBridge rules permitted to invoke fn, via its resource policy."""
    rules = []
    try:
        pol = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        for st in pol.get("Statement", []):
            src = (st.get("Condition", {}).get("ArnLike", {})
                   .get("AWS:SourceArn", ""))
            if "rule/" in str(src):
                rname = str(src).split("rule/")[-1]
                try:
                    rd = events.describe_rule(Name=rname)
                    rules.append({"rule": rname, "state": rd.get("State"),
                                  "cron": rd.get("ScheduleExpression")})
                except Exception:
                    rules.append({"rule": rname, "state": "describe-failed"})
    except lam.exceptions.ResourceNotFoundException:
        return "no-resource-policy"
    except Exception as e:
        return f"policy-err:{str(e)[:80]}"
    return rules


def recent_logs(fn, n=20):
    try:
        lg = f"/aws/lambda/{fn}"
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True,
            limit=1).get("logStreams", [])
        if not streams:
            return ["(no log streams)"]
        ev = logs.get_log_events(
            logGroupName=lg, logStreamName=streams[0]["logStreamName"],
            limit=n, startFromHead=False).get("events", [])
        return [e["message"].strip()[:180] for e in ev][-n:]
    except Exception as e:
        return [f"(log read failed: {str(e)[:100]})"]


def diagnose(fn):
    d = {"function": fn}
    try:
        c = lam.get_function_configuration(FunctionName=fn)
        d["exists"] = True
        d["last_modified"] = c.get("LastModified")
        d["state"] = c.get("State")
        d["runtime"] = c.get("Runtime")
    except Exception as e:
        d["exists"] = False
        d["err"] = str(e)[:140]
        return d
    d["schedule"] = schedule_for(fn)
    d["invocations_14d"] = metric_sum(fn, "Invocations")
    d["errors_14d"] = metric_sum(fn, "Errors")
    d["recent_logs"] = recent_logs(fn)
    return d


# 1. diagnose the candidate engines
report["engines"] = {}
for fn in ("justhodl-feedback", "justhodl-pre-pump-detector",
           "justhodl-dex-scanner"):
    report["engines"][fn] = diagnose(fn)

# 2. orphan check — is each stale output referenced by a live page / produced anywhere?
html_files = glob.glob("*.html") + glob.glob("**/*.html", recursive=True)
src_files = (glob.glob("aws/lambdas/*/source/*.py")
             + glob.glob("cloudflare/**/*.js", recursive=True)
             + glob.glob(".github/workflows/*.yml"))


def references(name, files):
    hits = []
    for f in files:
        try:
            txt = open(f, encoding="utf-8", errors="ignore").read()
            if name in txt:
                hits.append(f)
        except Exception:
            pass
    return hits[:6]


report["outputs"] = {}
for out in ("dex-scanner-data", "skew", "institutional-convergence",
            "pre-pump-calibration", "feedback-summary", "user-trades",
            "user-watchlist", "history-api-url"):
    page_refs = references(out + ".json", html_files)
    producer_refs = [f for f in references('"' + out + '"', src_files)
                     + references(out + ".json", src_files)]
    age_h = None
    try:
        head = s3.head_object(Bucket="justhodl-dashboard-live",
                              Key=f"data/{out}.json")
        age_h = round((datetime.now(timezone.utc)
                       - head["LastModified"]).total_seconds() / 3600, 1)
    except Exception:
        pass
    if producer_refs:
        verdict = "HAS PRODUCER — investigate engine"
    elif page_refs:
        verdict = "ORPHANED but still referenced by a live page — archive carefully"
    else:
        verdict = "ORPHANED — no producer, no live page; safe to archive"
    report["outputs"][out] = {
        "age_hours": age_h, "producer_in_code": producer_refs,
        "referenced_by_pages": page_refs, "verdict": verdict}

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/776_stale_output_triage.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/776_stale_output_triage.json")
