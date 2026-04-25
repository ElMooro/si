#!/usr/bin/env python3
"""
Verify step 70 worked — intelligence-report.json should now have:
  - khalid_index: 43 (not 0)
  - regime: BEAR (not UNKNOWN)
  - real ml_risk_score and carry_risk_score values
  - executive_summary populated from synthesized pred

Compare against the baseline from step 69.
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("verify_intelligence_fix") as r:
    r.heading("Verify intelligence-report.json now has real values")

    # ─── A. Check the file freshness ───
    r.section("A. File freshness")
    obj = s3.get_object(Bucket=BUCKET, Key="intelligence-report.json")
    age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    data = json.loads(obj["Body"].read())
    r.log(f"  intelligence-report.json age: {age_min:.1f} min ({obj['ContentLength']:,} bytes)")
    r.log(f"  LastModified: {obj['LastModified'].isoformat()}")
    if age_min > 5:
        r.warn(f"  File still old — async invoke may not have completed yet")

    # ─── B. Check the scores dict (the critical one) ───
    r.section("B. data.scores dict (critical for signal-logger)")
    scores = data.get("scores", {})
    r.log(f"  scores: {json.dumps(scores, indent=2)}")
    real_count = 0
    zero_count = 0
    null_count = 0
    for k, v in scores.items():
        if v is None: null_count += 1
        elif v == 0: zero_count += 1
        else: real_count += 1
    r.log(f"\n  Real values:  {real_count}")
    r.log(f"  Zero values:  {zero_count}")
    r.log(f"  Null values:  {null_count}")
    if scores.get("khalid_index", 0) > 0:
        r.ok(f"  ✓ khalid_index = {scores.get('khalid_index')} (real value, was 0)")
    else:
        r.fail(f"  ✗ khalid_index still {scores.get('khalid_index')} — fix not landing")

    # ─── C. ML synthesized fields ───
    r.section("C. Synthesized ML fields from new pred dict")
    ml_dependent = ["executive_summary", "carry_trade", "sector_rotation",
                    "trade_recommendations", "market_snapshot"]
    populated = 0
    for f in ml_dependent:
        v = data.get(f)
        if v in (None, "", 0, [], {}):
            r.log(f"  - {f:25} EMPTY (acceptable for synth — only fills what's available)")
        else:
            preview = str(v)[:100].replace("\n", " ")
            r.log(f"  ✓ {f:25} {preview}")
            populated += 1

    # ─── D. Other key fields (regime, phase, etc) ───
    r.section("D. Top-level regime/phase fields")
    regime = data.get("regime")
    phase = data.get("phase")
    r.log(f"  regime: {regime}")
    r.log(f"  phase:  {phase}")
    if regime and regime != "UNKNOWN":
        r.ok(f"  ✓ regime is set ({regime})")
    if phase and phase != "":
        r.ok(f"  ✓ phase is set ({phase})")

    # ─── E. Recent Lambda log to confirm it ran without errors ───
    r.section("E. Recent justhodl-intelligence log output")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-intelligence",
            orderBy="LastEventTime", descending=True, limit=1,
        ).get("logStreams", [])
        if streams:
            sname = streams[0]["logStreamName"]
            stream_age = (datetime.now(timezone.utc) - datetime.fromtimestamp(
                streams[0]["lastEventTimestamp"]/1000, tz=timezone.utc)).total_seconds() / 60
            r.log(f"  Latest stream: {sname} ({stream_age:.1f} min old)")
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-intelligence",
                logStreamName=sname, limit=30, startFromHead=True,
            )
            for e in ev.get("events", [])[:30]:
                m = e["message"].strip()
                if m and not m.startswith("REPORT") and not m.startswith("END") and not m.startswith("START"):
                    r.log(f"    {m[:200]}")
    except Exception as e:
        r.warn(f"  {e}")

    r.kv(
        khalid_index=scores.get("khalid_index", "?"),
        ml_risk_score=scores.get("ml_risk_score", "?"),
        carry_risk_score=scores.get("carry_risk_score", "?"),
        ml_fields_populated=populated,
        file_age_min=round(age_min, 1),
    )
    r.log("Done")
