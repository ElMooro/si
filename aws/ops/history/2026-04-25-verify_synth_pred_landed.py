#!/usr/bin/env python3
"""
Step 76 — final check after step 75 (synth_pred derives ml_risk +
carry_risk from real signals).

Expected after step 75:
  scores.khalid_index        = 43        (since step 70)
  scores.plumbing_stress     = 25        (since step 73)
  scores.ml_risk_score       = ~edge composite score (NEW)
  scores.carry_risk_score    = ~plumbing stress score (NEW)
"""
import json
from datetime import datetime, timezone
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("verify_synth_pred_landed") as r:
    r.heading("Verify ml_risk + carry_risk now have real values")

    # First check the source data — what's in edge-data.composite_score?
    r.section("Source data sanity check")
    try:
        edge_obj = s3.get_object(Bucket=BUCKET, Key="edge-data.json")
        edge = json.loads(edge_obj["Body"].read())
        edge_score = edge.get("composite_score", "?")
        edge_age_h = (datetime.now(timezone.utc) - edge_obj["LastModified"]).total_seconds() / 3600
        r.log(f"  edge-data.json composite_score: {edge_score} ({edge_age_h:.1f}h old)")
    except Exception as e:
        r.warn(f"  edge-data: {e}")

    try:
        repo_obj = s3.get_object(Bucket=BUCKET, Key="repo-data.json")
        repo = json.loads(repo_obj["Body"].read())
        stress = repo.get("stress", {})
        r.log(f"  repo-data.json stress: score={stress.get('score')} status={stress.get('status')}")
    except Exception as e:
        r.warn(f"  repo-data: {e}")

    # Now intelligence-report.json
    r.section("intelligence-report.json scores")
    obj = s3.get_object(Bucket=BUCKET, Key="intelligence-report.json")
    age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    data = json.loads(obj["Body"].read())
    scores = data.get("scores", {})

    r.log(f"  Age: {age_min:.1f} min, size: {obj['ContentLength']:,} bytes")
    r.log(f"  scores: {json.dumps(scores, indent=4)}")

    real = 0
    fields = ["khalid_index", "plumbing_stress", "ml_risk_score", "carry_risk_score", "vix"]
    for k in fields:
        v = scores.get(k)
        if v not in (None, 0, "0", ""):
            r.log(f"  ✓ {k} = {v}")
            real += 1
        else:
            r.log(f"  ✗ {k} = {v}")

    r.kv(
        scores_real=f"{real}/{len(fields)}",
        ml_risk_score=scores.get("ml_risk_score"),
        carry_risk_score=scores.get("carry_risk_score"),
    )

    if real >= 4:
        r.ok(f"  ✅ {real}/{len(fields)} critical scores are real values")
    elif real >= 3:
        r.warn(f"  ⚠ {real}/{len(fields)} — partial")
    else:
        r.fail(f"  ✗ Only {real}/{len(fields)} real")

    r.log("Done")
