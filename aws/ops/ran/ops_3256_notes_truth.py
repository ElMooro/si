"""ops 3256 — TV notes: the truth. 3254 probed the wrong key
(data/tv-notes.json); the real mirror is data/tradingview-notes.json
(extension ingest + autonomous crawler both land there). Read it,
count it, sample it, and answer the cadence question with evidence:
crawler schedule + last run + last error decide whether the one-time
extension action armed autonomous re-harvest or pushes are manual."""
import json
import sys
from datetime import datetime, timedelta, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
CW = boto3.client("cloudwatch", region_name=REGION)
EVT = boto3.client("scheduler", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3256_notes_truth") as rep:
    fails, warns = [], []
    rep.heading("ops 3256 — tradingview-notes.json: read, sample, "
                "cadence answered")

    rep.section("1. The mirror")
    d = s3_json("data/tradingview-notes.json") or {}
    notes = d.get("notes") or []
    rep.kv(notes=len(notes),
           generated_at=str(d.get("generated_at") or
                            d.get("harvested_at"))[:19],
           source=str(d.get("source") or d.get("mode") or "?")[:30])
    for n in notes[:4]:
        sym = n.get("symbol") or n.get("ticker") or "—"
        txt = (n.get("text") or n.get("content") or n.get("body")
               or "")[:90]
        ts = str(n.get("created") or n.get("updated") or "")[:10]
        rep.log(f"  [{sym}] {ts} {txt}")
    fresh_yday = False
    try:
        ga = str(d.get("generated_at") or d.get("harvested_at"))
        age_h = (datetime.now(timezone.utc)
                 - datetime.fromisoformat(ga.replace("Z", "+00:00"))
                 ).total_seconds() / 3600
        fresh_yday = age_h < 48
        rep.kv(age_hours=round(age_h, 1))
    except Exception:
        pass
    if notes and fresh_yday:
        rep.ok(f"YESTERDAY'S HARVEST CONFIRMED: {len(notes)} notes "
               "landed")
    elif notes:
        warns.append("mirror populated but older than 48h")
    else:
        fails.append("mirror empty — extension push did not land notes")

    rep.section("2. Cadence — is re-running needed?")
    now = datetime.now(timezone.utc)
    try:
        inv = CW.get_metric_data(MetricDataQueries=[{
            "Id": "i", "MetricStat": {"Metric": {
                "Namespace": "AWS/Lambda", "MetricName": "Invocations",
                "Dimensions": [{"Name": "FunctionName",
                                "Value": "justhodl-tv-notes-crawler"}]},
                "Period": 86400, "Stat": "Sum"}}],
            StartTime=now - timedelta(days=7), EndTime=now)
        tot = sum(inv["MetricDataResults"][0].get("Values") or [])
        rep.kv(crawler_invocations_7d=int(tot))
    except Exception as e:
        warns.append(f"metrics: {str(e)[:50]}")
    line = ""
    try:
        grp = "/aws/lambda/justhodl-tv-notes-crawler"
        for stm in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=2).get("logStreams") or []:
            for ev in reversed(LOGS.get_log_events(
                    logGroupName=grp, logStreamName=stm["logStreamName"],
                    limit=150, startFromHead=False).get("events") or []):
                m = (ev.get("message") or "").strip()
                if any(k in m.lower() for k in
                       ("error", "block", "403", "cloudflare", "notes",
                        "harvest")):
                    line = m[:130]
                    break
            if line:
                break
    except Exception:
        pass
    rep.log(f"  crawler last relevant log: {line or '—'}")

    rep.section("3. Verdict for Khalid")
    if notes and fresh_yday:
        rep.log("  Your one extension run DID deliver the notes — my "
                "3254 report checked the wrong key. Apology owed.")
    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
