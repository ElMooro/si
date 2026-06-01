"""1128 — measure JSON sizes loaded by index.html."""
import json, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1128_sizes.json"
s3 = boto3.client("s3", region_name="us-east-1")

# All keys index.html fetches via SOURCES
KEYS = {
    # Existing (pre-this-session)
    "signalBoard":   "data/signal-board.json",
    "intel":         "intel/current.json",
    "edge":          "edge-data.json",
    "liq":           "liquidity-data.json",
    "flow":          "flow-data.json",
    "crypto":        "crypto-intel.json",
    "regime":        "regime/current.json",
    "div":           "divergence/current.json",
    "cot":           "cot/extremes/current.json",
    "risk":          "risk/recommendations.json",
    "setups":        "opportunities/asymmetric-equity.json",
    "pnl":           "portfolio/pnl-daily.json",
    # NEW added this session
    "ppBrief":          "data/pump-radar-brief.json",
    "ppPositioning":    "data/pump-positioning.json",
    "ppCatalysts":      "data/catalysts.json",
    "ppClusters":       "data/catalyst-clusters.json",
    "ppEarly":          "data/velocity-acceleration.json",
}

NEW_KEYS = {"ppBrief","ppPositioning","ppCatalysts","ppClusters","ppEarly"}

def main():
    results = {}
    total_old = 0
    total_new = 0
    for key, s3key in KEYS.items():
        try:
            h = s3.head_object(Bucket="justhodl-dashboard-live", Key=s3key)
            sz = h["ContentLength"]
            results[key] = {
                "s3_key":         s3key,
                "size_bytes":     sz,
                "size_kb":        round(sz/1024, 1),
                "cache_control":  h.get("CacheControl", ""),
                "is_new":         key in NEW_KEYS,
            }
            if key in NEW_KEYS: total_new += sz
            else: total_old += sz
        except Exception as e:
            results[key] = {"s3_key": s3key, "error": str(e)[:200], "is_new": key in NEW_KEYS}
    
    summary = {
        "n_existing":    sum(1 for k in KEYS if k not in NEW_KEYS),
        "n_new_added":   len(NEW_KEYS),
        "total_old_kb":  round(total_old/1024, 1),
        "total_new_kb":  round(total_new/1024, 1),
        "ratio_pct":     round(100 * total_new / max(1, total_old), 1) if total_old else None,
    }
    
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "per_key": results,
    }
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1128] DONE")

if __name__ == "__main__":
    main()
