"""ops 3352 — probe the dispersed GLOBAL-RISK signals to fold into the JSI overlay:
European fragmentation / BTP-Bund, carry-unwind fragility, sovereign spreads, global tide,
capital cross-border. Lock the exact headline field + scale for each. Read-only.
"""
import json
import boto3
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# feed -> candidate dotted paths to a numeric stress/score
TARGETS = {
    "data/euro-fragmentation.json": ["score", "fragmentation.score", "fragmentation",
                                     "composite", "fragmentation_score"],
    "data/carry-surface.json":      ["unwind_overlay.cohort_fragility",
                                     "unwind_overlay.regime_multiplier"],
    "data/sovereign-stress.json":   ["score", "composite", "sovereign_spreads.composite",
                                     "spread_bp", "value"],
    "data/global-tide.json":        ["score", "composite", "value"],
    "data/capital-inflows.json":    ["score", "cross_border.score", "composite", "net_flow"],
    "data/crisis-composite.json":   ["btp_bund_canary.spread_bp", "btp_bund_canary.z",
                                     "btp_bund_canary", "master_crisis_score"],
    "data/canary-grid.json":        ["btp_bund", "composite_score", "score"],
    "data/euro-fragmentation.json2":["spread_vs_bund_bp"],  # dummy re-probe alt
}


def dig(o, path):
    cur = o
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return "MISS"
    return cur


with report("3352_global_risk_probe") as r:
    for key, paths in TARGETS.items():
        real_key = key.replace(".json2", ".json")
        try:
            o = json.loads(s3.get_object(Bucket=BUCKET, Key=real_key)["Body"].read().decode())
        except Exception as e:
            r.log(f"  ✗ {real_key}: {type(e).__name__}")
            continue
        found = False
        for p in paths:
            v = dig(o, p)
            if isinstance(v, (int, float)):
                r.log(f"  ✓ {real_key} :: {p} = {round(float(v),4)}")
                found = True
                break
        if not found:
            # show top keys + any nested numeric near stress-ish names
            top = list(o.keys())[:12] if isinstance(o, dict) else str(type(o))
            r.log(f"  ? {real_key}: top={top}")
            # one-level scan for numeric scores
            if isinstance(o, dict):
                for k, val in o.items():
                    if isinstance(val, (int, float)) and any(t in k.lower() for t in
                            ("score", "spread", "frag", "stress", "composite", "gauge", "bp", "z")):
                        r.log(f"      → candidate: {k} = {val}")
                    if isinstance(val, dict):
                        for k2, v2 in val.items():
                            if isinstance(v2, (int, float)) and any(t in (k+k2).lower() for t in
                                    ("score", "spread", "frag", "btp", "bund", "composite", "bp")):
                                r.log(f"      → candidate: {k}.{k2} = {v2}")
