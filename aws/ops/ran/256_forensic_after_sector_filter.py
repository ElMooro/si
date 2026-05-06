#!/usr/bin/env python3
"""Step 256 — Re-invoke forensic-screen after the financial-sector filter."""
import json, os, time, boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-forensic-screen"
SUMMARY_KEY = "data/forensic-screen.json"
REPORT_PATH = "aws/ops/reports/256_forensic_after_sector_filter.json"


def main():
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # Wait for redeploy + invoke
    deadline = time.time() + 60
    while time.time() < deadline:
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
        if resp.get("FunctionError"):
            print(f"  err {resp.get('FunctionError')} - waiting 10s")
            time.sleep(10)
            continue
        time.sleep(2)
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
        s = body.get("summary") or {}
        if "n_financial_sector_excluded_from_most_concerning" in s:
            print(f"  ✓ new code is live (financial filter present)")
            break
        print(f"  old code still — waiting 8s")
        time.sleep(8)

    body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
    s = body.get("summary") or {}
    most = body.get("most_concerning_top_25") or []

    out = {
        "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "generated_at": body.get("generated_at"),
        "n_scored": body.get("n_scored_ok"),
        "n_financial_excluded": s.get("n_financial_sector_excluded_from_most_concerning"),
        "n_m_flagged": s.get("n_m_flagged"),
        "n_sloan_flagged": s.get("n_sloan_flagged"),
        "n_wc_divergence_flagged": s.get("n_wc_divergence_flagged"),
        "n_goodwill_bloat_flagged": s.get("n_goodwill_bloat_flagged"),
        "top_5_concerning_after_filter": [
            {
                "symbol": r["symbol"],
                "sector": r.get("sector"),
                "concern_score": r.get("concern_score"),
                "m_score": r.get("m_score"),
                "sloan_accruals": r.get("sloan_accruals"),
                "wc_divergence": r.get("wc_divergence"),
                "goodwill_pct": r.get("goodwill_pct"),
            }
            for r in most[:5]
        ],
    }
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
