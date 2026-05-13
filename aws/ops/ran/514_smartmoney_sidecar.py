#!/usr/bin/env python3
"""514 — Verify the OTHER 13F sidecar (screener/smart-money.json) for page render."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/514_smartmoney_sidecar.json"
s3 = boto3.client("s3", region_name="us-east-1")


def check(key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        info = {"exists": True, "size_kb": round(len(body)/1024, 1),
                 "modified": obj["LastModified"].isoformat()[:19]}
        try:
            p = json.loads(body)
            info["top_keys"] = list(p.keys())[:25]
            info["generated_at"] = p.get("generated_at")
            info["as_of_quarter"] = p.get("as_of_quarter")
            # Sample lists
            filers = p.get("filers") or []
            if filers:
                info["n_filers"] = len(filers)
                info["top_3_filers"] = [
                    {"name": f.get("investor_name") or f.get("name"),
                      "cik": f.get("cik"),
                      "mv": f.get("market_value") or f.get("mv"),
                      "qoq_pct": f.get("qoq_change_pct") or f.get("qoq_pct"),
                      "added": f.get("securities_added") or f.get("added"),
                      "removed": f.get("securities_removed") or f.get("removed"),
                      "portfolio_size": f.get("portfolio_size")}
                    for f in filers[:3]
                ]
            summary = p.get("summary") or {}
            info["summary_keys"] = list(summary.keys())
            info["biggest_gainers_top3"] = [
                {"name": x.get("name"), "qoq_pct": x.get("qoq_pct"),
                  "mv": x.get("mv")}
                for x in (summary.get("biggest_gainers") or [])[:3]
            ]
            info["biggest_decliners_top3"] = [
                {"name": x.get("name"), "qoq_pct": x.get("qoq_pct")}
                for x in (summary.get("biggest_decliners") or [])[:3]
            ]
        except Exception as e:
            info["parse_err"] = str(e)[:100]
        return info
    except s3.exceptions.NoSuchKey:
        return {"exists": False}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["screener/smart-money.json"] = check("screener/smart-money.json")
    # Also peek at 13f-positions.json again with deeper sampling
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/13f-positions.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["data/13f-positions.json"] = {
            "size_kb": round(len(body)/1024, 1),
            "as_of_quarter": p.get("as_of_quarter"),
            "funds_parsed": p.get("funds_parsed"),
            "n_funds": len(p.get("by_fund") or {}),
            "n_tickers": len(p.get("aggregate_by_ticker") or {}),
            "fund_keys_sample": list((p.get("by_fund") or {}).keys())[:10],
            "most_bought_top_5": (p.get("most_bought") or [])[:5],
            "most_sold_top_5": (p.get("most_sold") or [])[:5],
            "consensus_holds_top_5": (p.get("consensus_holds") or [])[:5],
        }
    except Exception as e:
        out["13f_positions_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
