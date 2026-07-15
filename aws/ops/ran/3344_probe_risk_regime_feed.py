"""ops 3344 — probe: why did the unwind overlay see risk-regime=None? Check the S3 key
exists, its age, and its top-level shape so we fix the join (feed missing vs read bug)."""
import json
import boto3
from datetime import datetime, timezone
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

CANDIDATES = ["data/risk-regime.json", "data/eurodollar-stress.json",
              "data/eurodollar-plumbing.json", "data/firm-risk-board.json",
              "data/polygon-fx-regime.json"]

with report("3344_probe_risk_regime_feed") as r:
    for key in CANDIDATES:
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            age_h = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read().decode())
            top = list(d.keys()) if isinstance(d, dict) else f"(type {type(d).__name__})"
            r.ok(f"{key} — {head['ContentLength']}B, {age_h:.1f}h old")
            r.log(f"  top keys: {top[:16]}")
            if isinstance(d, dict):
                for probe in ("score", "regime", "generated_at", "posture", "stress_score"):
                    if probe in d:
                        r.log(f"  {probe} = {json.dumps(d[probe])[:100]}")
                res = d.get("results")
                if isinstance(res, dict):
                    vix = (res.get("vix") or {})
                    r.log(f"  results.vix.vix = {vix.get('vix')}")
        except s3.exceptions.NoSuchKey:
            r.fail(f"{key} — NoSuchKey (feed missing)")
        except Exception as e:
            r.fail(f"{key} — {type(e).__name__}: {str(e)[:100]}")
