#!/usr/bin/env python3
"""542 — Inspect insider-trades + insider-clusters + vix-curve + options-flow
sidecars to understand existing working data structures."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/542_working_sidecar_inspection.json"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def grab(key, n_keys=30):
    try:
        full = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = full["Body"].read()
        p = json.loads(body)
        out = {
            "size_kb": round(len(body) / 1024, 1),
            "top_keys": list(p.keys())[:n_keys],
        }
        # Pull a few summary fields
        for f in ("version", "generated_at", "composite_regime", "composite_signal",
                  "n_tickers", "n_with_data", "n_cluster_buys", "n_cluster_sells",
                  "total_buy_value_30d_usd", "total_sell_value_30d_usd",
                  "n_clusters", "n_buys", "n_sells",
                  "regime", "signal", "method", "schema_version",
                  "front_iv", "spot_iv", "vix", "vix9d", "vix3m", "vix6m",
                  "term_structure_regime", "contango_pct", "back_pct"):
            if f in p: out[f] = p[f]
        # Sample arrays
        for f in ("cluster_buys", "biggest_buy_dollars_30d", "biggest_sell_dollars_30d",
                  "top_clusters", "top_buys", "top_sells", "clusters", "trades",
                  "results", "all_qualifying", "alerts", "tier_a", "tier_a_names"):
            if f in p:
                v = p[f]
                if isinstance(v, list): out[f"{f}_sample"] = v[:3]
                elif isinstance(v, dict): out[f"{f}_keys"] = list(v.keys())[:10]
        # Sample by_ticker if exists
        if "by_ticker" in p:
            bt = p["by_ticker"]
            if isinstance(bt, dict):
                out["by_ticker_keys"] = list(bt.keys())[:10]
                samples = {}
                for k, v in list(bt.items())[:3]: samples[k] = v
                out["by_ticker_sample"] = samples
        return out
    except s3.exceptions.NoSuchKey:
        return {"exists": False}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    out["vix_curve"] = grab("data/vix-curve.json")
    out["vix_curve_history"] = grab("data/vix-curve-history.json")
    out["options_flow"] = grab("data/options-flow.json")
    out["options_gamma"] = grab("data/options-gamma.json")
    out["insider_trades"] = grab("data/insider-trades.json")
    out["insider_clusters"] = grab("data/insider-clusters.json")
    out["insider_transactions"] = grab("data/insider-transactions.json")

    # List ALL lambdas matching insider/vix/options for cross-reference
    out["lambdas_match"] = {}
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            n = fn["FunctionName"].lower()
            if any(k in n for k in ("insider", "vix", "option")):
                out["lambdas_match"][fn["FunctionName"]] = {
                    "last_modified": fn.get("LastModified"),
                    "runtime": fn.get("Runtime"),
                    "memory": fn.get("MemorySize"),
                    "timeout": fn.get("Timeout"),
                }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
