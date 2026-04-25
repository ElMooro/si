#!/usr/bin/env python3
"""
Decision support for ml-predictions: inspect what intelligence-report.json
actually contains right now. If the ML-derived fields are missing/empty,
the impact of ml-predictions being broken is real and we should fix it.
If those fields look fine (extracted from elsewhere), retire is safer.
"""
import json
from datetime import datetime, timezone
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("intelligence_report_inspection") as r:
    r.heading("intelligence-report.json — degraded or fine?")

    # Read the file
    obj = s3.get_object(Bucket=BUCKET, Key="intelligence-report.json")
    age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    data = json.loads(obj["Body"].read())
    r.log(f"  Size: {obj['ContentLength']:,} bytes ({age_min:.1f} min old)")
    r.log(f"  Top-level keys: {sorted(list(data.keys()))[:25]}")
    r.log("")

    # Check the ML-derived fields specifically
    r.section("Fields that come from predictions.json via justhodl-intelligence")

    ml_dependent = {
        "executive_summary": "Strategic narrative from ML",
        "ml_liquidity":      "ML's liquidity analysis",
        "ml_risk":           "ML's risk decomposition",
        "carry_trade":       "ML's carry analysis",
        "sector_rotation":   "ML's sector picks",
        "trade_recommendations": "ML's trade ideas",
        "market_snapshot":   "ML market overview",
        "us_equities":       "ML US equity outlook",
        "global_markets":    "ML global outlook",
        "agents_online":     "ML agent count",
    }

    populated = 0
    empty = 0
    for field, desc in ml_dependent.items():
        val = data.get(field)
        # Try variant names too
        for variant in [field, field.replace("_", "-"), field.replace("ml_", "")]:
            if variant in data:
                val = data[variant]
                break
        if val in (None, "", 0, [], {}):
            r.log(f"  ✗ {field:25} EMPTY/MISSING — {desc}")
            empty += 1
        else:
            preview = str(val)[:80].replace("\n", " ")
            r.log(f"  ✓ {field:25} populated: {preview} — {desc}")
            populated += 1

    # Critical for signal-logger
    r.section("Fields signal-logger extracts (ml_risk, carry_risk)")
    scores = data.get("scores", {})
    if scores:
        r.log(f"  data.scores: {json.dumps(scores, indent=2)[:500]}")
    else:
        r.log(f"  ✗ data.scores MISSING — signal-logger can't extract ml_risk_score / carry_risk_score")

    # And phase
    phase = data.get("phase", "")
    r.log(f"\n  data.phase: '{phase}' (signal-logger uses this for market_phase signal)")

    r.section("Summary")
    r.log(f"  ML-dependent fields populated: {populated}/{len(ml_dependent)}")
    r.log(f"  ML-dependent fields empty:     {empty}/{len(ml_dependent)}")
    r.log("")
    if empty >= len(ml_dependent) * 0.7:
        r.warn("  ⚠ MOST ML fields are empty — predictions.json staleness IS impacting")
        r.warn("    intelligence-report.json. Retire is the wrong call. Need to fix the")
        r.warn("    pipeline.")
    elif populated >= len(ml_dependent) * 0.7:
        r.ok(f"  ✓ Most fields populated — ML fields are present (maybe via other paths).")
        r.ok(f"    Retire is safe — predictions.json broken doesn't break downstream.")
    else:
        r.log("  Mixed — partial degradation. Decision is judgment call.")

    r.kv(
        ml_fields_populated=populated,
        ml_fields_empty=empty,
        size_kb=round(obj['ContentLength']/1024, 1),
        age_min=round(age_min, 1),
    )
    r.log("Done")
