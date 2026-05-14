#!/usr/bin/env python3
"""548 — Inspect existing aggregator sidecars to check if any already covers the
15 Bloomberg-Gap regimes. If yes, no new page needed."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/548_existing_aggregator_audit.json"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

# Sidecars consumed by existing pages
KEYS = [
    ("regime-anomaly", "data/regime-anomaly.json"),
    ("macro-nowcast", "data/macro-nowcast.json"),
    ("cross-asset-regime", "data/cross-asset-regime.json"),
    ("compound-signals", "data/compound-signals.json"),
    # The 15 Bloomberg-Gap modules
    ("dealer-gex", "data/dealer-gex.json"),
    ("vix-curve", "data/vix-curve.json"),
    ("crypto-funding", "data/crypto-funding.json"),
    ("credit-stress", "data/credit-stress.json"),
    ("retail-sentiment", "data/retail-sentiment.json"),
    ("news-velocity", "data/news-velocity.json"),
    ("cb-stance", "data/cb-stance.json"),
    ("global-markets", "data/global-markets.json"),
    ("commodity-curves", "data/commodity-curves.json"),
    ("insider-clusters", "data/insider-clusters.json"),
    ("options-flow", "data/options-flow.json"),
    ("dix-history", "data/dix-history.json"),
    ("finra-short", "data/finra-short.json"),
    ("13f-positions", "data/13f-positions.json"),
    ("earnings-nlp", "data/earnings-nlp.json"),
    # Potential others
    ("khalid-index", "data/khalid-index.json"),
    ("risk-dashboard", "data/risk-dashboard.json"),
    ("daily-summary", "data/daily-summary.json"),
]


def inspect(label, key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        info = {
            "key": key,
            "exists": True,
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
        }
        try:
            p = json.loads(body)
            info["top_level_keys"] = list(p.keys())[:25]
            # Find regime/state/composite fields
            regime_fields = {}
            def walk(node, path=""):
                if isinstance(node, dict):
                    for k, v in node.items():
                        full = f"{path}.{k}" if path else k
                        if any(token in k.lower() for token in [
                            "regime", "composite", "signal", "state", "stance",
                            "_classification", "level"
                        ]) and isinstance(v, (str, int, float)):
                            regime_fields[full] = str(v)[:80]
                        if isinstance(v, (dict, list)) and len(path.split(".")) < 3:
                            walk(v, full)
                elif isinstance(node, list) and node and isinstance(node[0], dict):
                    walk(node[0], path + "[0]")
            walk(p)
            info["regime_signal_fields"] = regime_fields
        except Exception as e:
            info["parse_err"] = str(e)[:120]
        return info
    except Exception as e:
        return {"key": key, "exists": False, "err": str(e)[:120]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "sidecars": {}}

    for label, key in KEYS:
        out["sidecars"][label] = inspect(label, key)

    # Check if any Lambda is named meta-regime / composite-regime / 15-regime
    try:
        paginator = lam.get_paginator("list_functions")
        meta_lambdas = []
        for page in paginator.paginate():
            for f in page.get("Functions", []):
                n = f["FunctionName"]
                if any(token in n.lower() for token in [
                    "meta-regime", "composite-regime", "regime-aggr",
                    "cross-asset", "compound-signal", "regime-detect",
                    "regime-classifier", "regime-meta", "regime-comp",
                    "bloomberg-gap-aggr", "all-regimes"
                ]):
                    meta_lambdas.append({
                        "name": n, "memory": f.get("MemorySize"),
                        "modified": f.get("LastModified"),
                    })
        out["potential_aggregator_lambdas"] = meta_lambdas
    except Exception as e:
        out["lambda_inv_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
