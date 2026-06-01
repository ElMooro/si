"""1101 — read live auction-crisis.json + show ALL forward_calendar entries with concession info."""
import json, pathlib
from datetime import datetime, timezone
import boto3
REPORT = "aws/ops/reports/1101_live_check.json"
s3 = boto3.client("s3", region_name="us-east-1")

def main():
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")
    d = json.loads(obj["Body"].read())
    
    out = {
        "schema":         d.get("schema_version"),
        "last_modified":  obj["LastModified"].isoformat(),
        "size_kb":        round(obj["ContentLength"]/1024, 1),
    }
    
    fwd = d.get("forward_calendar") or []
    out["forward_count"] = len(fwd)
    
    # Show EVERY entry's concession data
    out["all_entries"] = []
    for i, f in enumerate(fwd):
        out["all_entries"].append({
            "i":              i,
            "auction_date":   f.get("auction_date"),
            "security_type":  f.get("security_type"),
            "security_term":  f.get("security_term"),
            "days_ahead":     f.get("days_ahead"),
            "offering_billions": f.get("offering_amount_billions"),
            "concession_series":  f.get("concession_series"),
            "concession_5d_bp":   f.get("concession_5d_bp"),
            "concession_1d_bp":   f.get("concession_1d_bp"),
            "concession_regime":  f.get("concession_regime"),
            "concession_today_yield": f.get("concession_today_yield"),
            "concession_interpretation": (f.get("concession_interpretation") or "")[:120],
        })
    
    # Count by regime
    by_regime = {}
    for e in out["all_entries"]:
        r = e.get("concession_regime") or "NONE"
        by_regime[r] = by_regime.get(r, 0) + 1
    out["regime_counts"] = by_regime
    
    # Also check the LIVE JS via cdnetc check
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1101] DONE — {out['regime_counts']}")

if __name__ == "__main__":
    main()
