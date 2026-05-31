#!/usr/bin/env python3
"""1066 — final ARK state snapshot. Reads S3 + shows full data."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1066_ark_final.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ark-holdings.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["size_kb"] = round(len(body) / 1024, 1)
        out["schema"] = d.get("schema_version")
        out["method"] = d.get("method")
        out["generated_at"] = d.get("generated_at")
        out["duration_s"] = d.get("duration_s")
        out["data_source"] = d.get("data_source")
        out["n_funds"] = d.get("n_funds_fetched")
        out["n_positions"] = d.get("n_positions_total")
        out["n_unique"] = d.get("n_unique_tickers")
        
        out["fund_breakdown"] = {
            fund: {
                "n_positions": len(positions),
                "top_3": [
                    {"ticker": p["ticker"],
                     "weight": round(p["weight"], 2),
                     "value": round(p["market_value"])}
                    for p in positions[:3]
                ]
            }
            for fund, positions in (d.get("holdings_by_fund") or {}).items()
        }
        
        out["top_15_cross_fund"] = [
            {"ticker": r["ticker"],
             "company": (r.get("company") or "")[:30],
             "n_funds": r["n_funds"],
             "total_value": r["total_value"],
             "max_weight": round(r.get("max_weight", 0), 2),
             "funds_in": [f["fund"] for f in r.get("funds", [])]}
            for r in (d.get("cross_fund_top") or [])[:15]
        ]
        
        diff = d.get("diff_vs_prev", {})
        out["diff_vs_prev"] = {
            "n_new":    diff.get("n_new_positions", 0),
            "n_adds":   diff.get("n_position_adds", 0),
            "n_trims":  diff.get("n_position_trims", 0),
            "n_closed": diff.get("n_closed_positions", 0),
        }
    except Exception as e:
        out["err"] = str(e)[:300]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1066] DONE → {REPORT}")


if __name__ == "__main__":
    main()
