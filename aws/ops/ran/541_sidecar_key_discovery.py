#!/usr/bin/env python3
"""541 — Find true sidecar keys for VIX Term v2, options-flow-scanner, and
diagnose why insider-transactions returns zeros."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/541_sidecar_key_discovery.json"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── List all data/* sidecars in S3 ───
    print("Listing data/* sidecars...")
    paginator = s3.get_paginator("list_objects_v2")
    sidecars = []
    for page in paginator.paginate(Bucket="justhodl-dashboard-live", Prefix="data/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                sidecars.append({"key": obj["Key"], "size_kb": round(obj["Size"]/1024, 1),
                                  "modified": obj["LastModified"].isoformat()[:19]})

    out["all_data_sidecars"] = sorted(sidecars, key=lambda x: x["modified"], reverse=True)
    out["n_data_sidecars"] = len(sidecars)
    print(f"Found {len(sidecars)} sidecars")

    # ─── Specifically look for VIX-related keys ───
    out["vix_keys"] = [s for s in sidecars if "vix" in s["key"].lower()]
    out["options_keys"] = [s for s in sidecars if "option" in s["key"].lower() or "gex" in s["key"].lower() or "gamma" in s["key"].lower()]
    out["insider_keys"] = [s for s in sidecars if "insider" in s["key"].lower()]

    # ─── Investigate insider transactions Lambda details ───
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-insider-transactions")
        out["insider_lambda"] = {
            "exists": True,
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "env_keys": sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys()),
        }
        # Look at the source code briefly via S3 if downloadable, or just confirm the env var keys
        # Check the sidecar contents
        try:
            full = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-transactions.json")
            body = full["Body"].read()
            p = json.loads(body)
            out["insider_sidecar"] = {
                "size_kb": round(len(body)/1024, 1),
                "version": p.get("version"),
                "n_tickers": p.get("n_tickers"),
                "n_with_data": p.get("n_with_data"),
                "n_with_err": p.get("n_with_err"),
                "n_cluster_buys": p.get("n_cluster_buys"),
                "total_buy_value_30d_usd": p.get("total_buy_value_30d_usd"),
                "total_sell_value_30d_usd": p.get("total_sell_value_30d_usd"),
                "composite_regime": p.get("composite_regime"),
                "composite_signal": p.get("composite_signal"),
                "by_ticker_sample": dict(list((p.get("by_ticker") or {}).items())[:3]),
                "first_error_sample": [t for t in (p.get("by_ticker") or {}).values()
                                          if isinstance(t, dict) and t.get("err")][:3],
                "top_keys": list(p.keys())[:25],
            }
        except Exception as e:
            out["insider_sidecar_err"] = str(e)[:200]
    except Exception as e:
        out["insider_lambda_err"] = str(e)[:200]

    # ─── Pull deeper detail from a sampled insider ticker ───
    try:
        full = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-transactions.json")
        p = json.loads(full["Body"].read())
        by_ticker = p.get("by_ticker") or {}
        # Show ALL tickers and their values to figure out why zeros
        samples = {}
        for tkr, info in list(by_ticker.items())[:10]:
            samples[tkr] = info
        out["insider_per_ticker_sample"] = samples
    except Exception as e:
        out["insider_deep_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
