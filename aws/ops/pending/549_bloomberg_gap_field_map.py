#!/usr/bin/env python3
"""549 — Map exact regime/signal field paths across all 15 Bloomberg-Gap sidecars
so the upcoming regime-composite Lambda knows where to read each module's verdict."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/549_bloomberg_gap_field_map.json"

s3 = boto3.client("s3", region_name="us-east-1")

# All 15 Bloomberg-Gap modules
SIDECARS = [
    "data/dealer-gex.json",
    "data/vix-curve.json",
    "data/crypto-funding.json",
    "data/credit-stress.json",
    "data/retail-sentiment.json",
    "data/news-velocity.json",
    "data/cb-stance.json",
    "data/global-markets.json",
    "data/commodity-curves.json",
    "data/insider-clusters.json",
    "data/options-flow.json",
    "data/dix-history.json",
    "data/finra-short.json",
    "data/13f-positions.json",
    "data/earnings-nlp.json",
]


def walk_for_regime(node, path="", out=None, depth=0):
    """Recursively find any key containing 'regime', 'composite', 'signal',
    'state', 'stance', 'label' that points to a string OR a small dict."""
    if out is None: out = {}
    if depth > 4: return out
    if isinstance(node, dict):
        for k, v in node.items():
            full = f"{path}.{k}" if path else k
            keylc = k.lower()
            if any(tok in keylc for tok in [
                "regime", "composite", "signal", "stance", "state",
                "_classification", "level", "verdict", "narrative",
                "summary"
            ]):
                if isinstance(v, str) and len(v) < 200:
                    out[full] = {"type": "str", "value": v[:160]}
                elif isinstance(v, (int, float)):
                    out[full] = {"type": "num", "value": v}
                elif isinstance(v, dict) and len(v) < 12:
                    # Surface a small dict's keys
                    out[full] = {"type": "dict", "keys": list(v.keys())[:10],
                                  "sample": {kk: (str(vv)[:80] if not isinstance(vv,(dict,list)) else type(vv).__name__) for kk, vv in list(v.items())[:5]}}
                elif isinstance(v, list) and v and isinstance(v[0], str):
                    out[full] = {"type": "list_str", "value": v[:5]}
            if isinstance(v, (dict, list)):
                walk_for_regime(v, full, out, depth + 1)
    elif isinstance(node, list) and node:
        if isinstance(node[0], dict):
            walk_for_regime(node[0], path + "[0]", out, depth + 1)
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "sidecars": {}}

    for key in SIDECARS:
        info = {"key": key}
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            info["size_kb"] = round(len(body) / 1024, 1)
            info["modified"] = obj["LastModified"].isoformat()[:19]
            try:
                p = json.loads(body)
                info["top_level_keys"] = list(p.keys())[:20]
                info["regime_fields"] = walk_for_regime(p)
                # Also extract any top-level summary string of decent length
                for k, v in p.items():
                    if isinstance(v, str) and 20 < len(v) < 400 and "summary" not in info.get("regime_fields", {}):
                        if k.lower() in ("description", "narrative", "tldr", "synopsis"):
                            info.setdefault("top_summary", {})[k] = v[:300]
            except Exception as e:
                info["parse_err"] = str(e)[:120]
        except Exception as e:
            info["err"] = str(e)[:120]
        out["sidecars"][key] = info

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
