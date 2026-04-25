#!/usr/bin/env python3
"""
Step 74 — Final verification of the ml-predictions chain.

After step 73 (boto3 switch), intelligence-report.json should now have:
  - khalid_index = 43 (already good after step 70)
  - ml_risk_score, carry_risk_score: NON-ZERO real values
  - executive_summary, sector_rotation populated from synth_pred
  - regime, phase populated

If all 3 score fields are real (not 0), the ml-predictions decision is
fully complete. Signal-logger's next run will log real ml_risk and
carry_risk signals instead of poisoning the calibration data with zeros.
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("verify_ml_chain_final") as r:
    r.heading("Final verification — entire ml-predictions chain healthy")

    # ─── A. Latest intelligence-report.json ───
    r.section("A. intelligence-report.json — fresh + real values?")
    obj = s3.get_object(Bucket=BUCKET, Key="intelligence-report.json")
    age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    data = json.loads(obj["Body"].read())
    r.log(f"  Age: {age_min:.1f} min, size: {obj['ContentLength']:,} bytes")

    scores = data.get("scores", {})
    r.log(f"\n  scores dict:")
    r.log(json.dumps(scores, indent=4))

    critical = ["khalid_index", "ml_risk_score", "carry_risk_score", "plumbing_stress"]
    real_count = 0
    for k in critical:
        v = scores.get(k)
        if v not in (None, 0, "0", ""):
            r.log(f"  ✓ {k} = {v}")
            real_count += 1
        else:
            r.log(f"  ✗ {k} = {v}")

    # ─── B. ML synthesized fields ───
    r.section("B. Synthesized ML fields populated?")
    populated = 0
    for f in ["executive_summary", "carry_trade", "sector_rotation",
              "trade_recommendations", "market_snapshot"]:
        v = data.get(f)
        if v in (None, "", 0, [], {}):
            r.log(f"  - {f}: empty")
        else:
            preview = str(v)[:120].replace("\n", " ")
            r.log(f"  ✓ {f}: {preview}")
            populated += 1

    # ─── C. Recent log lines from latest invocation ───
    r.section("C. Latest justhodl-intelligence log output")
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
            errors = []
            for e in ev.get("events", [])[:30]:
                m = e["message"].strip()
                if m and not m.startswith("REPORT") and not m.startswith("END") and not m.startswith("START"):
                    if "ERR" in m or "FAILED" in m:
                        errors.append(m)
                    r.log(f"    {m[:200]}")
            r.log(f"\n  Error lines in this run: {len(errors)}")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── D. Trigger signal-logger to capture clean ml_risk values ───
    r.section("D. Trigger signal-logger — next batch should have real ml_risk")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-signal-logger",
            InvocationType="Event",
        )
        r.ok(f"  Async-triggered signal-logger (status {resp['StatusCode']})")
        r.log("  Next run reads fresh intelligence-report.json with real scores")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    r.kv(
        critical_scores_real=f"{real_count}/4",
        ml_fields_populated=f"{populated}/5",
        khalid_index=scores.get("khalid_index"),
        ml_risk_score=scores.get("ml_risk_score"),
        carry_risk_score=scores.get("carry_risk_score"),
    )

    if real_count == 4:
        r.ok("  ✅ All 4 critical scores have real values — ml chain healthy")
    elif real_count >= 2:
        r.warn(f"  Partial: {real_count}/4 critical scores real")
    else:
        r.fail(f"  Still broken: only {real_count}/4 critical scores real")

    r.log("Done")
