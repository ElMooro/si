#!/usr/bin/env python3
"""566 — Read 13F sidecar in full: schema, as_of_quarter, per-fund filing
dates. Q1 2026 13F filing deadline is today (2026-05-15) — check which funds
have already filed vs still on Q4 2025."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/566_13f_schema_freshness.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Full sidecar inspection
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/13f-positions.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar_size_kb"] = round(len(body) / 1024, 1)
        out["sidecar_modified"] = obj["LastModified"].isoformat()[:19]
        out["top_level_keys"] = list(p.keys())
        out["as_of_quarter"] = p.get("as_of_quarter")
        out["generated_at"] = p.get("generated_at")
        out["funds_total"] = p.get("funds_total")
        out["funds_parsed"] = p.get("funds_parsed")
        out["funds_failed"] = p.get("funds_failed")
        out["fund_errors"] = p.get("fund_errors")
        out["fetch_duration_s"] = p.get("fetch_duration_s")

        # Per-fund detail
        by_fund = p.get("by_fund") or {}
        fund_details = {}
        for name, fdata in by_fund.items():
            if not isinstance(fdata, dict): continue
            fund_details[name] = {
                "all_keys": list(fdata.keys()),
                "filing_date": fdata.get("filing_date") or fdata.get("filed_date"),
                "report_date": fdata.get("report_date") or fdata.get("period"),
                "as_of": fdata.get("as_of") or fdata.get("as_of_quarter") or fdata.get("quarter"),
                "n_holdings_top": len(fdata.get("top_holdings", [])) if isinstance(fdata.get("top_holdings"), list) else 0,
                "n_holdings_full": len(fdata.get("holdings", [])) if isinstance(fdata.get("holdings"), list) else 0,
                "total_value": fdata.get("total_value") or fdata.get("portfolio_value"),
                "n_positions": fdata.get("n_positions"),
                "changes_summary_keys": list((fdata.get("changes_summary") or {}).keys()),
                "n_new_positions": (fdata.get("changes_summary") or {}).get("n_new"),
                "n_exits": len((fdata.get("changes_summary") or {}).get("exits") or [])
                           if isinstance((fdata.get("changes_summary") or {}).get("exits"), list)
                           else (fdata.get("changes_summary") or {}).get("n_exits"),
            }
            # First holding sample
            top_h = fdata.get("top_holdings") or fdata.get("holdings") or []
            if isinstance(top_h, list) and top_h:
                fund_details[name]["first_holding"] = top_h[0]
        out["per_fund_detail"] = fund_details

        # Net action scores summary
        most_bought = p.get("most_bought") or []
        most_sold = p.get("most_sold") or []
        out["top_5_buys"] = [
            {"ticker": x.get("ticker"), "name": x.get("name"),
              "n_funds_holding": x.get("n_funds_holding"),
              "n_adding": x.get("n_funds_adding"),
              "n_trimming": x.get("n_funds_trimming"),
              "n_new": x.get("n_funds_new_position"),
              "n_exiting": x.get("n_funds_exiting"),
              "net_action_score": x.get("net_action_score"),
              "total_value_usd": x.get("total_value")}
            for x in most_bought[:10]
        ]
        out["top_5_sells"] = [
            {"ticker": x.get("ticker"), "name": x.get("name"),
              "n_funds_holding": x.get("n_funds_holding"),
              "n_adding": x.get("n_funds_adding"),
              "n_trimming": x.get("n_funds_trimming"),
              "n_new": x.get("n_funds_new_position"),
              "n_exiting": x.get("n_funds_exiting"),
              "net_action_score": x.get("net_action_score"),
              "total_value_usd": x.get("total_value")}
            for x in most_sold[:10]
        ]
    except Exception as e:
        out["err"] = str(e)[:300]
        import traceback
        out["trace"] = traceback.format_exc()[:1000]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
