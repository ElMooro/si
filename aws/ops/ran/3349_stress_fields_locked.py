"""ops 3349 — precise headline-score extraction using the CORRECT field names found in
3348's top-keys. Locks the exact field + observed scale for each feed so the unified
JustHodl Stress Index can normalize them onto a common 0-100 stress axis. Read-only.
"""
import json
import boto3
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# feed -> (dotted path to numeric score, note on scale/polarity)
FIELDS = {
    "data/global-stress.json":       ("global_stress_index", "0-100, higher=stress"),
    "data/ciss-stress.json":         ("ea_composite", "0-1 CISS, higher=stress"),
    "data/systemic-stress.json":     ("composite.value", "z or 0-100?"),
    "data/tail-risk.json":           ("system_tail_gauge", "0-100"),
    "data/vvix-vov-regime.json":     ("signal_strength", "0-1 or 0-100?"),
    "data/bank-stress.json":         ("bank_stress_score", "0-100"),
    "data/crisis-canaries.json":     ("composite_score", "0-100 or n firing"),
    "data/crisis-composite.json":    ("master_crisis_score", "0-100"),
    "data/liquidity-inflection.json":("composite.score", "?"),
    "data/eurodollar-stress.json":   ("composite_score", "0-100"),
    "data/eurodollar-plumbing.json": ("stress_score", "0-100"),
    "data/risk-regime.json":         ("risk_regime_score", "-100..100 RORO (invert)"),
    "data/polygon-fx-regime.json":   ("fx_roro.fx_roro_score", "-? RORO (invert)"),
    "data/tail-hedge.json":          ("severity", "label?"),
    "data/factor-risk.json":         ("headline", "label?"),
    "data/risk-radar.json":          ("macro_stress", "?"),
}


def dig(o, path):
    cur = o
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return "MISSING"
    return cur


with report("3349_stress_fields_locked") as r:
    r.section("Locked headline fields + scales")
    good = {}
    for key, (path, note) in FIELDS.items():
        try:
            o = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())
        except Exception as e:
            r.log(f"  ✗ {key}: {type(e).__name__}")
            continue
        v = dig(o, path)
        kind = type(v).__name__
        if isinstance(v, (int, float)):
            good[key] = {"field": path, "value": round(float(v), 4), "note": note}
            r.log(f"  ✓ {key} :: {path} = {round(float(v),4)}  [{note}]")
        else:
            # search one level for any numeric near-composite key
            alt = None
            if isinstance(o, dict):
                for k, val in o.items():
                    if isinstance(val, (int, float)) and any(t in k.lower() for t in ("score", "composite", "stress", "index", "gauge", "value", "level")):
                        alt = (k, val); break
            r.log(f"  ? {key} :: {path}={v}({kind})  alt={alt}")
    r.ok(f"{len(good)} feeds field-locked")
    r.log("LOCKED = " + json.dumps(good))
